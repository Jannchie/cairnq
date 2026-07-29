// Worker resilience. Each test here pins a way the run loop used to give up or
// leak rather than carry on:
//   - a store error while finalizing a task rejected a promise nobody awaited,
//     so Node killed the whole worker process on an unhandled rejection;
//   - a transient error from claim() propagated out of run() and ended the loop;
//   - every idle poll pushed a wakeup closure that was only ever freed by stop();
//   - a lost lease was invisible to the handler, which kept doing side effects
//     next to the task's new owner.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";

import { CairnQ, LostLease, Worker } from "../src/index.js";
import type { TaskContext } from "../src/context.js";
import { SQLiteStore } from "../src/store/sqlite.js";
import { freshDbPath, sleep, waitFor } from "./helpers.js";

let dbPath: string;
let client: CairnQ;

beforeEach(async () => {
  dbPath = freshDbPath();
  client = CairnQ.sqlite(dbPath);
  await client.connect();
});

afterEach(async () => {
  await client.close();
});

describe("worker resilience", () => {
  it("survives a store error while finalizing a task and reports it", async () => {
    const store = new SQLiteStore(dbPath);
    await store.connect();
    store.complete = async () => {
      throw new Error("disk I/O error");
    };
    const errors: unknown[] = [];
    const worker = new Worker(store, ["default"], {
      pollIntervalMs: 20,
      onError: (err) => errors.push(err),
    });
    worker.task("job", async () => ({ ok: true }));

    await worker.background(async () => {
      await client.submit("job", {});
      await waitFor(() => errors.length > 0);
    });
    await store.close();

    expect(errors.length).toBeGreaterThan(0);
    expect(String((errors[0] as Error).message)).toContain("disk I/O error");
  });

  it("keeps polling after a transient claim error", async () => {
    const store = new SQLiteStore(dbPath);
    await store.connect();
    const realClaim = store.claim.bind(store);
    let calls = 0;
    store.claim = async (input) => {
      calls += 1;
      if (calls <= 2) throw new Error("database is locked");
      return realClaim(input);
    };
    const errors: unknown[] = [];
    const worker = new Worker(store, ["default"], {
      pollIntervalMs: 10,
      onError: (err) => errors.push(err),
    });
    worker.task("job", async () => ({ ok: true }));

    const result = await worker.background(() =>
      client.call("job", {}, { waitTimeoutMs: 5_000, pollMs: 20 }),
    );
    await store.close();

    expect(result).toEqual({ ok: true });
    expect(errors.length).toBeGreaterThanOrEqual(2);
  });

  it("stop() cuts an idle sleep short instead of waiting out the poll", async () => {
    // The poll interval is far above the assertion: run() can only settle in
    // time if stop() resolves the sleep the worker is parked in. (The per-poll
    // wakeup registry this test used to inspect is gone — every sleep now
    // races one lifetime stopped-promise, so there is nothing to accumulate.)
    const worker = Worker.sqlite(dbPath, { queues: ["default"], pollIntervalMs: 5_000 });
    const runner = worker.run();
    await sleep(100); // let it park in its idle sleep
    const t0 = Date.now();
    worker.stop();
    await runner;
    await worker.close();

    expect(Date.now() - t0).toBeLessThan(1_000);
  });

  it("drains in-flight tasks however the run loop exits", async () => {
    // run() promises that when it settles, nothing it started is still running —
    // serve() closes the store the moment it returns, and a handler still holding
    // the connection would fault. The normal exits honour that; an unexpected
    // throw out of the loop body used to skip it and abandon the in-flight work.
    const store = new SQLiteStore(dbPath);
    await store.connect();
    const realClaim = store.claim.bind(store);
    let calls = 0;
    store.claim = async (input) => {
      calls += 1;
      if (calls === 1) return realClaim(input);
      return undefined as never; // a store that breaks its own contract
    };
    let finished = false;
    // Two slots, so the loop comes back around to the broken claim while the
    // first task is still running.
    const worker = new Worker(store, ["default"], { pollIntervalMs: 5, concurrency: 2 });
    worker.task("job", async () => {
      await sleep(200);
      finished = true;
      return {};
    });

    await client.submit("job", {});
    await expect(worker.run()).rejects.toThrow();
    expect(finished, "run() settled while a handler was still running").toBe(true);
    await store.close();
  });

  it("signals a lost lease to the running handler", async () => {
    const worker = Worker.sqlite(dbPath, {
      queues: ["default"],
      pollIntervalMs: 20,
      leaseMs: 5_000,
      heartbeatIntervalMs: 30,
    });
    let observed = false;
    let aborted = false;
    worker.task("job", async (ctx) => {
      ctx.signal.addEventListener("abort", () => {
        aborted = true;
      });
      for (let i = 0; i < 300 && !ctx.lostLease; i++) await sleep(10);
      observed = ctx.lostLease;
      return { ok: true };
    });

    await worker.background(async () => {
      const t = await client.submit("job", {});
      // Simulate another worker taking the task over mid-flight.
      await waitFor(async () => (await client.get(t.id))?.status === "running");
      const db = new Database(dbPath);
      db.prepare("update cairnq_tasks set worker_id = 'someone_else' where id = ?").run(t.id);
      db.close();
      await waitFor(() => observed, 3_000);
    });

    expect(observed).toBe(true);
    expect(aborted).toBe(true);
  });

  // ------------------------------------------------------------- maxRunMs
  // Why the ceiling exists: see WorkerOptions.maxRunMs.

  it("abandons a hung handler at maxRunMs and fails the task", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, maxRunMs: 100 });
    worker.task("hang", () => new Promise(() => {})); // never settles

    let final;
    await worker.background(async () => {
      const t = await client.submit("hang", {}, { maxAttempts: 1 });
      final = await client.wait(t.id, { timeoutMs: 3_000, pollMs: 20 });
    });
    expect(final!.status).toBe("failed");
    expect(final!.error?.code).toBe("handler_timeout");
  });

  it("retries a timed-out attempt", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, maxRunMs: 100, retryBackoffMs: 1 });
    const attempts: number[] = [];
    worker.task("flaky", async (ctx) => {
      attempts.push(ctx.attempt);
      if (ctx.attempt === 1) await new Promise(() => {}); // hang only once
      return { attempt: ctx.attempt };
    });

    let final;
    await worker.background(async () => {
      const t = await client.submit("flaky", {});
      final = await client.wait(t.id, { timeoutMs: 3_000, pollMs: 20 });
    });
    expect(final!.status).toBe("succeeded");
    expect(final!.result).toEqual({ attempt: 2 });
    expect(attempts).toEqual([1, 2]);
  });

  it("frees the concurrency slot after a timeout", async () => {
    // concurrency=1: the hung task occupies the only slot until the ceiling
    // abandons it; the queued task must then run rather than wait for a lease.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, maxRunMs: 100, leaseMs: 60_000 });
    worker.task("hang", () => new Promise(() => {}));
    worker.task("quick", async () => ({ ok: true }));

    await client.submit("hang", {}, { maxAttempts: 1 });
    await worker.background(async () => {
      await expect(client.call("quick", {}, { waitTimeoutMs: 3_000, pollMs: 20 })).resolves.toEqual(
        { ok: true },
      );
    });
  });

  it("cuts a zombie handler off from the store after a timeout", async () => {
    // The promise can't be cancelled, so the abandoned handler keeps running —
    // its ctx must abort and refuse every further write (why: TaskContext.owned).
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, maxRunMs: 100 });
    let zombieCtx: TaskContext | undefined;
    worker.task("hang", (ctx) => {
      zombieCtx = ctx;
      return new Promise(() => {});
    });

    await worker.background(async () => {
      const t = await client.submit("hang", {}, { maxAttempts: 1 });
      await client.wait(t.id, { timeoutMs: 3_000, pollMs: 20 });
    });
    expect(zombieCtx!.lostLease).toBe(true);
    expect(zombieCtx!.signal.aborted).toBe(true);
    await expect(zombieCtx!.progress(0.9)).rejects.toThrow(LostLease);
  });
});
