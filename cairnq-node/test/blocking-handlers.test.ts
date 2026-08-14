// Handlers that block, and the lease renewal that dies with them.
//
// The heartbeat runs on the worker's event loop. A handler that occupies that
// loop — a tight loop over a large array, a *Sync filesystem or crypto call —
// stops the renewal too: the lease expires, another worker recovers the task, and
// both compute the same thing. Nothing in the handler can see it happen, and Node
// has no way to preempt it, so the least it can do is say so.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, EventLoopBlocked, Worker } from "../src/index.js";
import { freshDbPath, waitFor } from "./helpers.js";

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

/** Occupy the loop for `ms`, the way real synchronous work does. */
function block(ms: number): void {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    // spin
  }
}

describe("a handler that blocks the event loop", () => {
  it("is reported through onError while the lease still holds", async () => {
    const errors: unknown[] = [];
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      leaseMs: 300,
      onError: (err) => errors.push(err),
    });
    // Three beats' worth at leaseMs 300 (interval = lease/3), so the beat that
    // never ran is unambiguous — and short, since a spin here really does take
    // the CPU away from everything else running beside it.
    worker.task("blocking", () => {
      block(300);
      return {};
    });

    await worker.background(async () => {
      await client.submit("blocking", {});
      await waitFor(() => errors.some((e) => e instanceof EventLoopBlocked));
    });
    await worker.close();

    const blocked = errors.filter((e): e is EventLoopBlocked => e instanceof EventLoopBlocked);
    expect(blocked.length).toBeGreaterThan(0);
    expect(blocked[0].lateMs).toBeGreaterThan(blocked[0].intervalMs);
    expect(blocked[0].message).toMatch(/event loop was blocked/);
  });

  it("says nothing about a handler that yields", async () => {
    const errors: unknown[] = [];
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      leaseMs: 300,
      onError: (err) => errors.push(err),
    });
    worker.task("slow", async () => {
      // Longer than a beat, but it gives the loop back.
      await new Promise((r) => setTimeout(r, 250));
      return {};
    });

    const done = await worker.background(async () => {
      const task = await client.submit("slow", {});
      return client.wait(task.id, { timeoutMs: 5_000 });
    });
    await worker.close();

    expect(done.status).toBe("succeeded");
    expect(errors.filter((e) => e instanceof EventLoopBlocked)).toEqual([]);
  });
});
