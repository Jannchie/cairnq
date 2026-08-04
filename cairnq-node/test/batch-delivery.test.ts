/**
 * Batch delivery: one handler call over several tasks.
 *
 * The contract under test is single — **when a batch handler returns, every task
 * it did not settle itself is settled by how the call ended** — plus the escape
 * hatch that makes it usable: a handler can settle individual tasks as it goes,
 * and the worker neither re-settles those nor keeps renewing their leases.
 *
 * The Python twin (tests/test_batch_delivery.py) asserts the same behaviors.
 */

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, isRunning, LostLease, TaskError, Worker } from "../src/index.js";
import type { Task } from "../src/index.js";
import { allTerminal, freshDbPath, sleep, waitFor } from "./helpers.js";

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

/** Run the worker until every id is terminal, then return the tasks by id. */
async function drain(ids: string[], worker: Worker) {
  await worker.background(async () => {
    await waitFor(() => allTerminal(client, ids), 5_000);
  });
  const out = new Map<string, Task>();
  for (const id of ids) out.set(id, (await client.get(id))!);
  return out;
}

async function submitMany(name: string, n: number, payload: (i: number) => any, opts: any = {}) {
  const ids: string[] = [];
  for (let i = 0; i < n; i++) ids.push((await client.submit(name, payload(i), opts)).id);
  return ids;
}

describe("batch delivery", () => {
  it("calls the handler once for the whole batch", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 8 });
    const calls: number[] = [];

    worker.task("embed", { batch: 8 }, async (items) => {
      calls.push(items.length);
      return Object.fromEntries(items.map((i) => [i.taskId, { n: i.payload.n }]));
    });

    const ids = await submitMany("embed", 5, (n) => ({ n }));
    const tasks = await drain(ids, worker);

    expect(calls).toEqual([5]);
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
    // The returned map fills in each task's own result.
    expect(new Set([...tasks.values()].map((t) => (t.result as any).n))).toEqual(
      new Set([0, 1, 2, 3, 4]),
    );
  });

  it("chunks a batch by its registered size", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 8 });
    const calls: number[] = [];

    worker.task("embed", { batch: 3 }, async (items) => {
      calls.push(items.length);
    });

    const ids = await submitMany("embed", 7, () => ({}));
    const tasks = await drain(ids, worker);

    expect(calls.reduce((a, b) => a + b, 0)).toBe(7);
    expect(Math.max(...calls)).toBeLessThanOrEqual(3);
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
  });

  it("returning nothing succeeds the whole batch with no result", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 4 });
    worker.task("index", { batch: 4 }, async () => {});

    const ids = await submitMany("index", 3, () => ({}));
    const tasks = await drain(ids, worker);

    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
    expect([...tasks.values()].every((t) => t.result === null)).toBe(true);
  });

  it("throwing fails every unsettled task retryably", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 4, retryBackoffMs: 0 });
    worker.task("flaky", { batch: 4 }, async () => {
      throw new Error("provider down");
    });

    const ids = await submitMany("flaky", 3, () => ({}), { maxAttempts: 1 });
    const tasks = await drain(ids, worker);

    expect([...tasks.values()].every((t) => t.status === "failed")).toBe(true);
    // Each task keeps its own error record and its own attempt count.
    expect([...tasks.values()].every((t) => (t.error as any).message === "provider down")).toBe(
      true,
    );
    expect([...tasks.values()].every((t) => t.attempt === 1)).toBe(true);
  });

  it("re-attempts a retryable batch failure per task", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 4, retryBackoffMs: 0 });
    let calls = 0;

    worker.task("flaky", { batch: 4 }, async () => {
      if (++calls === 1) throw new Error("transient");
    });

    const ids = await submitMany("flaky", 2, () => ({}), { maxAttempts: 3 });
    const tasks = await drain(ids, worker);

    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
    expect([...tasks.values()].every((t) => t.attempt === 2)).toBe(true);
  });

  it("throwing a non-retryable TaskError fails the rest permanently", async () => {
    // The `abort_for_credit_depletion` shape: one condition ends the whole batch
    // and nothing should be retried.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 4, retryBackoffMs: 0 });
    worker.task("translate", { batch: 4 }, async () => {
      throw new TaskError("credit depleted", { code: "credit_depleted", retryable: false });
    });

    const ids = await submitMany("translate", 3, () => ({}), { maxAttempts: 5 });
    const tasks = await drain(ids, worker);

    expect([...tasks.values()].every((t) => t.status === "failed")).toBe(true);
    expect([...tasks.values()].every((t) => (t.error as any).code === "credit_depleted")).toBe(
      true,
    );
    // Permanent: one attempt, despite maxAttempts: 5.
    expect([...tasks.values()].every((t) => t.attempt === 1)).toBe(true);
  });

  it("lets a handler settle some tasks and the rest ride on the return", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 8, retryBackoffMs: 0 });

    worker.task("embed", { batch: 8 }, async (items) => {
      for (const item of items) {
        if (item.payload.n % 2) await item.fail("odd is not embeddable", { retryable: false });
      }
      return Object.fromEntries(items.map((i) => [i.taskId, { n: i.payload.n }]));
    });

    const ids = await submitMany("embed", 6, (n) => ({ n }), { maxAttempts: 3 });
    const tasks = await drain(ids, worker);
    const byN = new Map([...tasks.values()].map((t) => [(t.payload as any).n, t]));

    expect([0, 1, 2, 3, 4, 5].map((n) => byN.get(n)!.status)).toEqual([
      "succeeded",
      "failed",
      "succeeded",
      "failed",
      "succeeded",
      "failed",
    ]);
    // The explicitly failed ones kept their own reason and were not retried,
    // even though the handler returned normally and maxAttempts allowed more.
    expect((byN.get(1)!.error as any).message).toBe("odd is not embeddable");
    expect(byN.get(1)!.attempt).toBe(1);
    // A task the handler settled is not overwritten by the batch's return value.
    expect(byN.get(1)!.result).toBeNull();
    expect(byN.get(0)!.result).toEqual({ n: 0 });
  });

  it("makes settling twice a no-op", async () => {
    // Handlers built on ack/nack queues all carry a `finalizedIds` set to
    // guarantee this. Holding it in the context is the point.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 4 });

    worker.task("once", { batch: 4 }, async (items) => {
      for (const item of items) {
        expect(await item.succeed({ first: true })).not.toBeNull();
        expect(item.settled).toBe(true);
        // Every later attempt is a no-op that reports it did nothing.
        expect(await item.succeed({ second: true })).toBeNull();
        expect(await item.fail("too late")).toBeNull();
      }
    });

    const ids = await submitMany("once", 2, () => ({}));
    const tasks = await drain(ids, worker);

    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
    expect([...tasks.values()].every((t) => (t.result as any).first === true)).toBe(true);
  });

  it("retries an explicitly failed task when retryable", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 4, retryBackoffMs: 0 });
    const seen: number[] = [];

    worker.task("retryable", { batch: 4 }, async (items) => {
      for (const item of items) {
        seen.push(item.attempt);
        if (item.attempt === 1) await item.fail("not yet", { retryable: true });
      }
    });

    const ids = await submitMany("retryable", 1, () => ({}), { maxAttempts: 3 });
    const tasks = await drain(ids, worker);

    expect(seen).toEqual([1, 2]);
    expect(tasks.get(ids[0])!.status).toBe("succeeded");
  });

  it("lets batch and single handlers share one worker", async () => {
    // A claim comes back mixed by name; each name is delivered its own way.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 8 });
    const batched: number[] = [];
    const singles: string[] = [];

    worker.task("embed", { batch: 8 }, async (items) => {
      batched.push(items.length);
    });
    worker.task("summarize", async (ctx) => {
      singles.push(ctx.taskId);
      return { ok: true };
    });

    const ids = [
      ...(await submitMany("embed", 4, () => ({}))),
      ...(await submitMany("summarize", 2, () => ({}))),
    ];
    const tasks = await drain(ids, worker);

    expect(batched.reduce((a, b) => a + b, 0)).toBe(4);
    expect(singles.length).toBe(2);
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
  });

  it("still uses the list form for batch: 1", async () => {
    // batch: 1 is a real configuration — work that saturates the machine (a
    // Docling parse) is registered this way, and must still get the list form.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20 });
    const shapes: number[] = [];

    worker.task("parse", { batch: 1 }, async (items) => {
      shapes.push(items.length);
    });

    const ids = await submitMany("parse", 3, () => ({}));
    const tasks = await drain(ids, worker);

    expect(shapes).toEqual([1, 1, 1]);
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
  });

  it("keeps every lease in the batch alive with one beat", async () => {
    // A handler outliving its lease must not have its tasks recovered under it —
    // and one beat has to cover the whole batch, not one task at a time.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      leaseMs: 200,
      heartbeatIntervalMs: 40,
    });

    worker.task("slow", { batch: 4 }, async (items) => {
      await sleep(600); // three lease lifetimes
      expect(items.some((i) => i.lostLease)).toBe(false);
    });

    const ids = await submitMany("slow", 3, () => ({}), { maxAttempts: 1 });
    const tasks = await drain(ids, worker);

    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
    // Never redelivered: a recovered lease would have burned the single attempt.
    expect([...tasks.values()].every((t) => t.attempt === 1)).toBe(true);
  });

  it("stops heartbeating a task the handler already settled", async () => {
    // Renewing a lease on a terminal row is a write against something nobody
    // owns; the beat has to drop tasks the handler already finished.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      leaseMs: 200,
      heartbeatIntervalMs: 40,
    });

    worker.task("half", { batch: 4 }, async (items) => {
      await items[0].succeed({ early: true });
      await sleep(300); // several beats, with one task already terminal
    });

    const ids = await submitMany("half", 2, () => ({}));
    const tasks = await drain(ids, worker);

    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
    expect([...tasks.values()].filter((t) => (t.result as any)?.early === true).length).toBe(1);
  });

  it("bounds the whole batch call with maxRunMs", async () => {
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      maxRunMs: 150,
      retryBackoffMs: 0,
    });

    // Never settles rather than sleeping: the worker abandons this attempt but
    // deliberately does not cancel the promise, so a real timer would outlive the
    // test and hold the runner's event loop open at teardown.
    worker.task("hang", { batch: 4 }, () => new Promise(() => {}));

    const ids = await submitMany("hang", 2, () => ({}), { maxAttempts: 1 });
    const tasks = await drain(ids, worker);

    expect([...tasks.values()].every((t) => t.status === "failed")).toBe(true);
    expect([...tasks.values()].every((t) => (t.error as any).code === "handler_timeout")).toBe(
      true,
    );
  });

  it("does not let a batched name start calls for an unbatched one", async () => {
    // Regression: the claim used to be one statement over every registered name,
    // so sizing it for the widest batch let a `batch: 64` registration pull 64
    // rows of unrelated work and turn each into its own call on a worker
    // configured for one. Each name now draws its own quota.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 1 });
    let live = 0;
    let peak = 0;

    worker.task("embed", { batch: 64 }, async () => {});
    worker.task("solo", async () => {
      live++;
      peak = Math.max(peak, live);
      await sleep(20);
      live--;
    });

    const ids = await submitMany("solo", 20, () => ({}));
    const tasks = await drain(ids, worker);

    expect(peak).toBe(1);
    expect([...tasks.values()].map((t) => t.status)).toEqual(Array(20).fill("succeeded"));
  });

  it("bounds concurrency by calls, not by tasks", async () => {
    // concurrency counts handler calls: a call holding 4 tasks is one of them.
    // Counting tasks instead is what used to weld batch size to concurrency —
    // a full batch was unreachable unless concurrency was raised to match it.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 2 });
    let calls = 0;
    let peakCalls = 0;
    let widest = 0;

    worker.task("embed", { batch: 4 }, async (items) => {
      calls++;
      peakCalls = Math.max(peakCalls, calls);
      widest = Math.max(widest, items.length);
      await sleep(30);
      calls--;
    });

    const ids = await submitMany("embed", 20, () => ({}));
    const tasks = await drain(ids, worker);

    expect(peakCalls).toBeLessThanOrEqual(2);
    // A full batch is reachable at concurrency 2 — 8 tasks in flight, 2 calls.
    expect(widest).toBe(4);
    expect([...tasks.values()].map((t) => t.status)).toEqual(Array(20).fill("succeeded"));
  });

  it("fills a batch on a worker left at the default concurrency", async () => {
    // The headline of the change: batch size is no longer capped by concurrency,
    // so `batch: 8` on a default worker delivers 8 rather than 1.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20 });
    const sizes: number[] = [];

    worker.task("embed", { batch: 8 }, async (items) => {
      sizes.push(items.length);
    });

    const ids = await submitMany("embed", 8, (n) => ({ n }));
    const tasks = await drain(ids, worker);

    expect(sizes).toEqual([8]);
    expect([...tasks.values()].map((t) => t.status)).toEqual(Array(8).fill("succeeded"));
  });

  it("caps calls per name with a per-name concurrency", async () => {
    // The worker budget allows 6 calls; `embed` may only ever run 2 of them, so
    // one expensive name cannot take the whole worker.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 6 });
    let live = 0;
    let peak = 0;

    worker.task("embed", { batch: 2, concurrency: 2 }, async () => {
      live++;
      peak = Math.max(peak, live);
      await sleep(30);
      live--;
    });

    const ids = await submitMany("embed", 20, () => ({}));
    const tasks = await drain(ids, worker);

    expect(peak).toBeLessThanOrEqual(2);
    expect([...tasks.values()].map((t) => t.status)).toEqual(Array(20).fill("succeeded"));
  });

  it("applies a per-name concurrency without batching", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, concurrency: 8 });
    let live = 0;
    let peak = 0;

    worker.task("slow", { concurrency: 1 }, async () => {
      live++;
      peak = Math.max(peak, live);
      await sleep(20);
      live--;
    });

    const ids = await submitMany("slow", 10, () => ({}));
    const tasks = await drain(ids, worker);

    expect(peak).toBe(1);
    expect([...tasks.values()].map((t) => t.status)).toEqual(Array(10).fill("succeeded"));
  });

  it("does not starve a name behind another name's backlog", async () => {
    // One slot, two backlogs. The claim serves groups in the order given, so
    // without rotating that order `embed` would hold the slot until its 40 tasks
    // were done and `other` would not run at all.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 5, concurrency: 1 });
    let embedDone = 0;
    let otherDone = 0;

    worker.task("embed", { batch: 4 }, async (items) => {
      embedDone += items.length;
    });
    worker.task("other", { batch: 4 }, async (items) => {
      otherDone += items.length;
    });

    const ids = [
      ...(await submitMany("embed", 40, () => ({}))),
      ...(await submitMany("other", 8, () => ({}))),
    ];
    await drain(ids, worker);

    expect(embedDone).toBe(40);
    // The real assertion is that this finished at all — a starved name would
    // leave drain() to time out.
    expect(otherDone).toBe(8);
  });

  it("does not read settling during a beat as lease loss", async () => {
    // Regression: the beat renews only rows still `running`, so a task the
    // handler settled while the beat was in flight comes back absent — which the
    // loop read as "another worker took it" and flagged the context lease-lost.
    // A handler checking lostLease was told to bail out after a clean succeed.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      leaseMs: 300,
      heartbeatIntervalMs: 10,
    });
    let flagged: boolean[] | undefined;

    worker.task("racy", { batch: 4 }, async (items) => {
      await sleep(50); // let a beat land first
      for (const item of items) await item.succeed({ ok: true });
      await sleep(100); // and several more beats after settling
      flagged = items.map((i) => i.lostLease);
    });

    const ids = await submitMany("racy", 3, () => ({}));
    const tasks = await drain(ids, worker);

    expect(flagged).toEqual([false, false, false]);
    expect([...tasks.values()].map((t) => t.status)).toEqual(Array(3).fill("succeeded"));
  });

  it("does not flag a single-task handler that settles early either", async () => {
    // The same rule through the single-task path, which shares the loop: it used
    // to heartbeat a terminal row every beat and flag the context on the first.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      leaseMs: 300,
      heartbeatIntervalMs: 10,
    });
    let flagged: boolean | undefined;

    worker.task("early", async (ctx) => {
      await ctx.succeed({ ok: true });
      await sleep(100); // several beats with the task already terminal
      flagged = ctx.lostLease;
    });

    const ids = await submitMany("early", 1, () => ({}));
    const tasks = await drain(ids, worker);

    expect(flagged).toBe(false);
    expect(tasks.get(ids[0])!.status).toBe("succeeded");
  });

  it("does not make a write after settling look like a lost lease", async () => {
    // `settled` gates every write through the context, not just the settlement
    // ones. Without that, progress() after a succeed() reaches the store, fails
    // the ownership check on a terminal row, and reports LostLease — telling the
    // handler another worker took its task when it had simply already finished
    // it, and flipping lostLease on the way out.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20 });
    let raised = false;
    let lost: boolean | undefined;

    worker.task("early", async (ctx) => {
      await ctx.succeed({ ok: true });
      try {
        await ctx.progress(0.5, "too late");
      } catch (err) {
        raised = err instanceof LostLease;
      }
      lost = ctx.lostLease;
    });

    const ids = await submitMany("early", 1, () => ({}));
    const tasks = await drain(ids, worker);

    expect(raised).toBe(true); // the write is refused, as before
    expect(lost).toBe(false); // but the lease state is not corrupted
    expect(tasks.get(ids[0])!.status).toBe("succeeded");
  });

  it("rejects a non-positive batch size at registration", () => {
    const worker = Worker.sqlite(dbPath);
    expect(() => worker.task("x", { batch: 0 }, async () => {})).toThrow(/batch must be/);
  });

  it("lets a single-task handler settle early", async () => {
    // succeed()/fail() are on TaskContext, not on anything batch-shaped, so they
    // work in single-task delivery too — there they mean "settle now". The worker
    // must then not complete the task a second time over the handler's decision.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20 });
    worker.task("early", async (ctx) => {
      await ctx.succeed({ decidedBy: "handler" });
      return { decidedBy: "return value" }; // ignored: already settled
    });

    const ids = await submitMany("early", 1, () => ({}));
    const tasks = await drain(ids, worker);

    expect(tasks.get(ids[0])!.status).toBe("succeeded");
    expect(tasks.get(ids[0])!.result).toEqual({ decidedBy: "handler" });
  });

  it("lets a single-task handler fail itself permanently", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, retryBackoffMs: 0 });
    worker.task("doomed", async (ctx) => {
      await ctx.fail("bad input", { retryable: false });
    });

    const ids = await submitMany("doomed", 1, () => ({}), { maxAttempts: 5 });
    const tasks = await drain(ids, worker);

    expect(tasks.get(ids[0])!.status).toBe("failed");
    expect((tasks.get(ids[0])!.error as any).message).toBe("bad input");
    expect(tasks.get(ids[0])!.attempt).toBe(1); // permanent, despite maxAttempts: 5
  });

  it("carries a cancel to a batch task through the shared heartbeat", async () => {
    // Cancellation rides along on the write the worker was making anyway — in a
    // batch that write is the shared beat, so it must carry each row back.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      leaseMs: 400,
      heartbeatIntervalMs: 30,
    });
    let observed: boolean | undefined;

    worker.task("cancellable", { batch: 4 }, async (items) => {
      // Watch the private flag, not canceled(): canceled() reads the row itself
      // when the flag is unset, so it would report true whether or not the beat
      // carried the cancel — which is the only thing this test is about.
      await waitFor(() => (items[0] as any).cancelSeen === true, 2_000);
      observed = (items[0] as any).cancelSeen;
      await sleep(50);
    });

    const target = await client.submit("cancellable", {});
    await worker.background(async () => {
      await waitFor(async () => {
        const t = await client.get(target.id);
        return t != null && isRunning(t);
      }, 2_000);
      await client.cancel(target.id);
      await waitFor(() => allTerminal(client, [target.id]), 3_000);
    });

    expect(observed).toBe(true);
    expect((await client.get(target.id))!.status).toBe("canceled");
  });
});
