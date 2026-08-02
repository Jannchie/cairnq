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

describe("queueDepth", () => {
  it("reports headroom against the limit, saturating at zero", async () => {
    expect(await client.queueDepth("default", 3)).toBe(3);
    await client.submit("job", {});
    expect(await client.queueDepth("default", 3)).toBe(2);
    await client.submit("job", {});
    await client.submit("job", {});
    expect(await client.queueDepth("default", 3)).toBe(0);
    // Past the limit it stays at 0 rather than going negative: the LIMIT
    // subquery is what keeps the scan bounded, and a gate only needs "no room".
    await client.submit("job", {});
    expect(await client.queueDepth("default", 3)).toBe(0);
  });

  it("counts queued only, so a claimed task frees headroom", async () => {
    await client.submit("job", {});
    expect(await client.queueDepth("default", 5)).toBe(4);

    // A running task has a worker and is bounded by that worker's concurrency;
    // the backlog worth pushing back on is work nobody has picked up.
    await client.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 5_000 });
    expect(await client.queueDepth("default", 5)).toBe(5);
  });

  it("counts delayed tasks — they are queued work that will run", async () => {
    await client.submit("job", {}, { runAtDelayMs: 60_000 });
    expect(await client.queueDepth("default", 5)).toBe(4);
  });

  it("is per queue", async () => {
    await client.submit("job", {}, { queue: "a" });
    await client.submit("job", {}, { queue: "a" });
    expect(await client.queueDepth("a", 5)).toBe(3);
    expect(await client.queueDepth("b", 5)).toBe(5);
  });

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
      const blocked = gated.submit("job", { i: 2 }).then(() => (released = true));
      await sleep(150);
      expect(released).toBe(false); // still waiting on a full queue

      worker.task("job", async () => ({ ok: true }));
      const runner = worker.run();
      await blocked;
      expect(released).toBe(true);

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
    const release: (() => void)[] = [];
    worker.task("job", async () => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise<void>((r) => release.push(r));
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
      release.forEach((r) => r());
      await waitFor(() => release.length === 4, 5_000);
      release.forEach((r) => r());
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
