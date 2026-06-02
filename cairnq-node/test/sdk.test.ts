import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";

import {
  CairnQ,
  defineTask,
  isFailed,
  isQueued,
  isSucceeded,
  isTerminal,
  LostLease,
  ProtocolVersionMismatch,
  TaskError,
  TaskFailed,
  TaskTimeout,
  Worker,
} from "../src/index.js";

let dbPath: string;
let client: CairnQ;

beforeEach(async () => {
  const dir = mkdtempSync(join(tmpdir(), "cairnq-"));
  dbPath = join(dir, "tasks.db");
  client = CairnQ.sqlite(dbPath);
  await client.connect();
});

afterEach(async () => {
  await client.close();
});

describe("client/worker", () => {
  it("call times out and leaves the task running", async () => {
    await expect(
      client.call("unhandled", {}, { waitTimeoutMs: 300, pollMs: 50 }),
    ).rejects.toBeInstanceOf(TaskTimeout);
    const tasks = await client.list({ name: "unhandled" });
    expect(tasks[0]?.status).toBe("queued");
  });

  it("runs a worker end-to-end via call", async () => {
    const worker = Worker.sqlite(dbPath, { queues: ["default"], concurrency: 2, pollIntervalMs: 20 });
    let seenAttempt = 0;
    worker.task("sum", async (ctx, payload) => {
      seenAttempt = ctx.attempt;
      await ctx.progress(0.5, "adding");
      return { sum: payload.a + payload.b };
    });
    const result = await worker.background(
      () => client.call("sum", { a: 2, b: 3 }, { waitTimeoutMs: 5000, pollMs: 20 }),
    );
    expect(result).toEqual({ sum: 5 });
    expect(seenAttempt).toBe(1);
  });

  it("retries a flaky handler then succeeds", async () => {
    const worker = Worker.sqlite(dbPath, { queues: ["default"], pollIntervalMs: 20, leaseMs: 5000 });
    const seen: number[] = [];
    worker.task("flaky", async (ctx) => {
      seen.push(ctx.attempt);
      if (ctx.attempt < 2) throw new Error("boom");
      return { ok: true };
    });
    const result = await worker.background(
      () => client.call("flaky", {}, { maxAttempts: 3, waitTimeoutMs: 5000, pollMs: 20 }),
    );
    expect(result).toEqual({ ok: true });
    expect(seen).toEqual([1, 2]);
  });

  it("wires parent/root for child tasks", async () => {
    const worker = Worker.sqlite(dbPath, { queues: ["default"], concurrency: 2, pollIntervalMs: 20 });
    worker.task("parent", async (ctx) => {
      const child = await ctx.submit("child", { v: 1 });
      return { childId: child.id };
    });
    worker.task("child", async (_ctx, payload) => ({ v: payload.v }));

    const { parentId, childId } = await worker.background(async () => {
      const parent = await client.submit("parent", {});
      const parentFinal = await client.wait(parent.id, { timeoutMs: 5000, pollMs: 20 });
      const childFinal = await client.wait((parentFinal.result as any).childId, {
        timeoutMs: 5000,
        pollMs: 20,
      });
      expect(childFinal.status).toBe("succeeded");
      return { parentId: parent.id, childId: (parentFinal.result as any).childId };
    });

    const childTask = await client.get(childId);
    expect(childTask?.parent_id).toBe(parentId);
    expect(childTask?.root_id).toBe(parentId);
    const chain = await client.list({ rootId: parentId });
    expect(new Set(chain.map((t) => t.id))).toEqual(new Set([parentId, childId]));
  });

  it("rejects zombie worker writes with LostLease", async () => {
    const store = client.store;
    const t = await client.submit("job", {});
    await store.claim({ queues: ["default"], workerId: "owner", leaseMs: 5000 });
    await expect(
      store.succeed({ taskId: t.id, workerId: "intruder", result: {} }),
    ).rejects.toBeInstanceOf(LostLease);
  });

  it("finalizes a cooperatively-canceled running task as canceled", async () => {
    const worker = Worker.sqlite(dbPath, { queues: ["default"], pollIntervalMs: 20, leaseMs: 5000 });
    let started = false;
    worker.task("longjob", async (ctx) => {
      started = true;
      for (let i = 0; i < 300; i++) {
        if (await ctx.canceled()) return; // cooperative exit
        await new Promise((r) => setTimeout(r, 10));
      }
      return { done: true };
    });
    const final = await worker.background(async () => {
      const t = await client.submit("longjob", {});
      while (!started) await new Promise((r) => setTimeout(r, 5));
      await client.cancel(t.id);
      return client.wait(t.id, { timeoutMs: 3000, pollMs: 20 });
    });
    expect(final.status).toBe("canceled"); // not "succeeded"
    expect(final.result).toBeNull();
  });

  it("fails permanently on TaskError (non-retryable)", async () => {
    const worker = Worker.sqlite(dbPath, { queues: ["default"], pollIntervalMs: 20 });
    const attempts: number[] = [];
    worker.task("bad", async (ctx) => {
      attempts.push(ctx.attempt);
      throw new TaskError("bad input", { code: "bad_input", retryable: false });
    });
    const err = await worker
      .background(() => client.call("bad", {}, { maxAttempts: 3, waitTimeoutMs: 3000, pollMs: 20 }))
      .catch((e) => e as TaskFailed);
    expect(err).toBeInstanceOf(TaskFailed);
    expect(err.code).toBe("bad_input"); // unpacked accessors, not err.error["code"]
    expect(err.message).toBe("bad input");
    expect(err.retryable).toBe(false);
    expect(attempts).toEqual([1]); // failed permanently, not retried
  });

  it("registers a handler under its function name and defaults payload to {}", async () => {
    const worker = Worker.sqlite(dbPath, { queues: ["default"], pollIntervalMs: 20 });
    worker.task(async function ping(ctx) {
      return { pong: true };
    });
    const result = await worker.background(() =>
      client.call("ping", undefined, { waitTimeoutMs: 3000, pollMs: 20 }),
    );
    expect(result).toEqual({ pong: true });
  });

  it("exposes status predicates on a task", async () => {
    const worker = Worker.sqlite(dbPath, { queues: ["default"], pollIntervalMs: 20 });
    worker.task(async function ok(_ctx) {
      return { done: true };
    });
    const t = await client.submit("ok", {});
    expect(isQueued(t)).toBe(true);
    expect(isTerminal(t)).toBe(false);
    const final = await worker.background(() =>
      client.wait(t.id, { timeoutMs: 3000, pollMs: 20 }),
    );
    expect(isSucceeded(final)).toBe(true);
    expect(isTerminal(final)).toBe(true);
    expect(isFailed(final)).toBe(false);
  });

  it("shares a typed task definition across worker and client", async () => {
    const sum = defineTask<{ a: number; b: number }, { sum: number }>("sum.typed");
    const worker = Worker.sqlite(dbPath, { queues: ["default"], pollIntervalMs: 20 });
    // payload is typed { a; b } from the def; no string repeated on either end.
    worker.task(sum, async (_ctx, payload) => ({ sum: payload.a + payload.b }));
    const result = await worker.background(() =>
      client.call(sum, { a: 2, b: 3 }, { waitTimeoutMs: 3000, pollMs: 20 }),
    );
    // `result` is inferred as { sum: number } via the TaskDef — this typed binding
    // would fail to compile if call() fell back to unknown.
    const typed: { sum: number } = result;
    expect(typed.sum).toBe(5);

    // A plain-string submit hits the same handler — the def is only the name.
    const again = await client.submit("sum.typed", { a: 1, b: 1 });
    expect(again.name).toBe("sum.typed");
  });

  it("recovers an expired lease on next claim", async () => {
    const store = client.store;
    const t = await client.submit("job", {}, { maxAttempts: 3 });
    const c1 = await store.claim({ queues: ["default"], workerId: "w1", leaseMs: 100 });
    expect(c1.length).toBe(1);
    await new Promise((r) => setTimeout(r, 200));
    const c2 = await store.claim({ queues: ["default"], workerId: "w2", leaseMs: 5000 });
    expect(c2.length).toBe(1);
    expect(c2[0].id).toBe(t.id);
    expect(c2[0].attempt).toBe(2);
    expect(c2[0].worker_id).toBe("w2");
  });

  it("never double-dispatches under concurrent claims", async () => {
    // Fire more concurrent single-claims than there are tasks: each task must be
    // claimed exactly once. better-sqlite3 is synchronous so these serialize on one
    // writer; the real contention check is this same assertion run against the
    // Postgres backend (FOR UPDATE SKIP LOCKED) once the conformance run is
    // parameterized over backends.
    const store = client.store;
    const n = 10;
    for (let i = 0; i < n; i++) await client.submit("job", { i });
    const batches = await Promise.all(
      Array.from({ length: n + 5 }, (_, k) =>
        store.claim({ queues: ["default"], workerId: `w${k}`, leaseMs: 5000, limit: 1 }),
      ),
    );
    const ids = batches.flat().map((t) => t.id);
    expect(ids.length).toBe(n); // exactly n dispatched, no phantom claims
    expect(new Set(ids).size).toBe(n); // and never the same task twice
  });

  it("refuses to connect on a protocol_version mismatch", async () => {
    const dir = mkdtempSync(join(tmpdir(), "cairnq-"));
    const badPath = join(dir, "bad.db");
    const raw = new Database(badPath);
    raw.exec("create table cairnq_meta (key text primary key, value text not null)");
    raw.prepare("insert into cairnq_meta values ('protocol_version', '999')").run();
    raw.close();
    const bad = CairnQ.sqlite(badPath);
    await expect(bad.connect()).rejects.toBeInstanceOf(ProtocolVersionMismatch);
    await bad.close();
  });
});
