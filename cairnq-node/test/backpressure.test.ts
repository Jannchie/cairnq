// Backpressure: the depth probe's semantics, the gate built on it, and the
// worker-side byte budget.
//
// Without these a producer that outruns its workers is bounded only by disk, and
// a worker sized by task count holds concurrency * largest-payload bytes the
// moment big payloads arrive. Each test here pins one half of that.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, QueueFull, Worker } from "../src/index.js";
import { QueueDepthGate } from "../src/backpressure.js";
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

// The statement's own semantics — headroom, saturation at 0, queued-only,
// per-queue isolation, delayed tasks counting — live in the conformance scenario
// (cairnq-protocol/conformance/scenarios/queue_depth.json), which runs in both
// SDKs against both dialects. Repeating them here would be strictly weaker
// coverage in a second place to maintain. What stays is SDK-side validation,
// which the SQL never sees.
describe("queueDepth", () => {
  it("rejects a negative limit rather than reading it as unbounded", async () => {
    await expect(client.queueDepth("default", -1)).rejects.toThrow(/non-negative/);
  });
});

describe("QueueDepthGate", () => {
  it("lets submits through while the queue is under its limit", async () => {
    const gated = CairnQ.sqlite(dbPath, { maxQueueDepth: 3 });
    await gated.connect();
    try {
      for (let i = 0; i < 3; i++) await gated.submit("job", { i });
      expect(await client.queueDepth("default", 10)).toBe(7);
    } finally {
      await gated.close();
    }
  });

  it("blocks at the limit and proceeds once a worker drains the queue", async () => {
    const gated = CairnQ.sqlite(dbPath, { maxQueueDepth: 2, queuePollIntervalMs: 20 });
    await gated.connect();
    const worker = new Worker(new SQLiteStore(dbPath), ["default"], { pollIntervalMs: 20 });
    try {
      await gated.submit("job", { i: 0 });
      await gated.submit("job", { i: 1 });

      let released = false;
      const blocked = gated.submit("job", { i: 2 });
      void blocked.then(() => (released = true));
      await sleep(150);
      expect(released).toBe(false); // still waiting on a full queue

      worker.task("job", async () => ({ ok: true }));
      const runner = worker.run();
      // Not that it resolved — awaiting it already says that — but that the task
      // the gate held back actually reached the queue once room appeared.
      const task = await blocked;
      expect(task.queue).toBe("default");
      expect(await client.get(task.id)).not.toBeNull();

      worker.stop();
      await runner;
    } finally {
      await worker.close();
      await gated.close();
    }
  });

  it("raises QueueFull on timeout and enqueues nothing", async () => {
    const gated = CairnQ.sqlite(dbPath, {
      maxQueueDepth: 1,
      maxQueueWaitMs: 120,
      queuePollIntervalMs: 20,
    });
    await gated.connect();
    try {
      await gated.submit("job", { i: 0 });
      await expect(gated.submit("job", { i: 1 })).rejects.toThrow(QueueFull);
      // The whole point of raising: the backlog did not grow past the limit.
      expect(await client.queueDepth("default", 10)).toBe(9);
    } finally {
      await gated.close();
    }
  });

  it("gates only the queues a per-queue limit names", async () => {
    const gated = CairnQ.sqlite(dbPath, {
      maxQueueDepth: { tight: 1 },
      maxQueueWaitMs: 60,
      queuePollIntervalMs: 20,
    });
    await gated.connect();
    try {
      await gated.submit("job", {}, { queue: "tight" });
      await expect(gated.submit("job", {}, { queue: "tight" })).rejects.toThrow(QueueFull);
      // "loose" is not listed, so it is not gated at all.
      for (let i = 0; i < 5; i++) await gated.submit("job", { i }, { queue: "loose" });
      expect(await client.queueDepth("loose", 10)).toBe(5);
    } finally {
      await gated.close();
    }
  });

  it("amortizes the probe across a grant instead of reading per submit", async () => {
    const store = new SQLiteStore(dbPath);
    await store.connect();
    let probes = 0;
    const realDepth = store.queueDepth.bind(store);
    store.queueDepth = async (queue, maxDepth) => {
      probes++;
      return realDepth(queue, maxDepth);
    };
    const gated = new CairnQ(store, { maxQueueDepth: 1_000 });
    try {
      for (let i = 0; i < 20; i++) await gated.submit("job", { i });
      // One probe's grant covers the run; the check is not a read per submit.
      expect(probes).toBe(1);
    } finally {
      await gated.close();
    }
  });

  it("shares one in-flight probe across concurrent submits", async () => {
    const store = new SQLiteStore(dbPath);
    await store.connect();
    let probes = 0;
    const realDepth = store.queueDepth.bind(store);
    store.queueDepth = async (queue, maxDepth) => {
      probes++;
      await sleep(20); // hold it open so the others must join rather than start their own
      return realDepth(queue, maxDepth);
    };
    const gated = new CairnQ(store, { maxQueueDepth: 1_000 });
    try {
      await Promise.all(Array.from({ length: 8 }, (_, i) => gated.submit("job", { i })));
      expect(probes).toBe(1);
      expect(await client.queueDepth("default", 100)).toBe(92);
    } finally {
      await gated.close();
    }
  });

  it("gates TaskContext.submit too — a handler spawning children is a producer", async () => {
    // "blocker" has no handler on this worker, so it is never claimed and holds
    // the queue at its limit for the whole test — no timing luck involved.
    await client.submit("parent", {});
    await client.submit("blocker", {});

    const worker = new Worker(new SQLiteStore(dbPath), ["default"], {
      concurrency: 1,
      pollIntervalMs: 10,
      maxQueueDepth: 1,
      maxQueueWaitMs: 100,
      queuePollIntervalMs: 20,
    });
    let caught: unknown;
    worker.task("parent", async (ctx) => {
      try {
        await ctx.submit("child", {});
      } catch (err) {
        caught = err;
      }
      return {};
    });
    const runner = worker.run();
    try {
      await waitFor(() => caught !== undefined, 3_000);
      expect(caught).toBeInstanceOf(QueueFull);
      // Gating the client alone would have let this through: a worker process
      // has no CairnQ handle, so the fan-out path would be the one unbounded one.
      expect(await client.list({ name: "child" })).toHaveLength(0);
    } finally {
      worker.stop();
      await runner;
      await worker.close();
    }
  });

  it("refuses a limit below 1 at construction, not at the first blocked submit", () => {
    const store = new SQLiteStore(dbPath);
    expect(() => new QueueDepthGate(store, { maxQueueDepth: 0 })).toThrow(/>= 1/);
    expect(() => new QueueDepthGate(store, { maxQueueDepth: { q: -1 } })).toThrow(/>= 1/);
  });
});

describe("maxInFlightBytes", () => {
  it("holds back claims on resident bytes, not just task count", async () => {
    // Four tasks, each ~64KB, against a budget of 100KB: the byte ceiling binds
    // before the concurrency one does, so the worker cannot run all four at once.
    const big = "x".repeat(64 * 1024);
    for (let i = 0; i < 4; i++) await client.submit("job", { i, big });

    const worker = new Worker(new SQLiteStore(dbPath), ["default"], {
      concurrency: 4,
      claimBatch: 1, // one task per claim, so the budget is consulted between them
      pollIntervalMs: 10,
      maxInFlightBytes: 100 * 1024,
    });
    let peak = 0;
    let inFlight = 0;
    // One gate for every handler, so teardown is a single release — stop() plus
    // run()'s finally drains whatever was still running.
    let openGate!: () => void;
    const gate = new Promise<void>((r) => (openGate = r));
    worker.task("job", async () => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await gate;
      inFlight--;
      return {};
    });
    const runner = worker.run();
    try {
      await waitFor(() => inFlight >= 1);
      await sleep(120); // give the loop every chance to over-claim
      // Two 64KB payloads already exceed 100KB, so the third cannot be claimed.
      expect(peak).toBeLessThanOrEqual(2);
      expect(peak).toBeGreaterThanOrEqual(1);
    } finally {
      openGate();
      worker.stop();
      await runner;
      await worker.close();
    }
  });

  it("runs a payload larger than the whole budget rather than deadlocking", async () => {
    // The budget is spent the moment this is charged, so nothing else claims
    // alongside it — but refusing to run it at all would stall the queue forever.
    await client.submit("job", { big: "x".repeat(200 * 1024) });
    const worker = new Worker(new SQLiteStore(dbPath), ["default"], {
      concurrency: 2,
      pollIntervalMs: 10,
      maxInFlightBytes: 10 * 1024,
    });
    let ran = false;
    worker.task("job", async () => {
      ran = true;
      return { ok: true };
    });
    const runner = worker.run();
    try {
      await waitFor(() => ran, 3_000);
      expect(ran).toBe(true);
    } finally {
      worker.stop();
      await runner;
      await worker.close();
    }
  });

  it("refunds the charge so a later task can still be claimed", async () => {
    const big = "x".repeat(64 * 1024);
    for (let i = 0; i < 3; i++) await client.submit("job", { i, big });
    const worker = new Worker(new SQLiteStore(dbPath), ["default"], {
      concurrency: 1,
      pollIntervalMs: 10,
      maxInFlightBytes: 80 * 1024,
    });
    let done = 0;
    worker.task("job", async () => {
      done++;
      return {};
    });
    const runner = worker.run();
    try {
      // Without the refund the budget stays spent after the first task and the
      // remaining two are never claimed.
      await waitFor(() => done === 3, 5_000);
      expect(done).toBe(3);
    } finally {
      worker.stop();
      await runner;
      await worker.close();
    }
  });
});
