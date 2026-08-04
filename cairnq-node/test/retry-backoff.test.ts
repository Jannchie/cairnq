// Retry backoff. The failure this pins: the worker recorded every failure with
// delayMs=0, so a task that keeps failing was re-claimed at poll speed and burned
// through maxAttempts in milliseconds — while README and PROTOCOL both promised
// "retries with backoff".
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, Worker } from "../src/index.js";
import { retryDelayMs } from "../src/worker.js";
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

describe("retry backoff", () => {
  it("doubles the jitter window per attempt and caps it", () => {
    const top = () => 1.0; // the open end of [0, 1)
    expect(retryDelayMs(1, 1_000, 30_000, top)).toBe(1_000);
    expect(retryDelayMs(2, 1_000, 30_000, top)).toBe(2_000);
    expect(retryDelayMs(3, 1_000, 30_000, top)).toBe(4_000);
    expect(retryDelayMs(20, 1_000, 30_000, top)).toBe(30_000);
  });

  // Equal jitter: never below half the window, never above it. The floor is what
  // keeps jitter from turning a long backoff into a fast retry; the spread is
  // what stops a fleet capped at maxMs from retrying on one beat.
  it("jitters over the upper half of the window", () => {
    expect(retryDelayMs(3, 1_000, 30_000, () => 0)).toBe(2_000);
    expect(retryDelayMs(3, 1_000, 30_000, () => 0.5)).toBe(3_000);

    const seen = new Set<number>();
    for (let i = 0; i < 200; i++) seen.add(retryDelayMs(20, 1_000, 30_000));
    expect(seen.size, "a capped backoff must not be a single deterministic beat").toBeGreaterThan(1);
    for (const d of seen) {
      expect(d).toBeGreaterThanOrEqual(15_000);
      expect(d).toBeLessThanOrEqual(30_000);
    }
  });

  it("treats a zero base as backoff disabled", () => {
    expect(retryDelayMs(3, 0, 30_000)).toBe(0);
  });

  it("requeues a failed attempt into the future", async () => {
    const worker = Worker.sqlite(dbPath, {
      queues: ["default"],
      pollIntervalMs: 20,
      retryBackoffMs: 2_000,
    });
    worker.task("flaky", async () => {
      throw new Error("boom");
    });

    const task = await worker.background(async () => {
      const t = await client.submit("flaky", {}, { maxAttempts: 3 });
      let cur = await client.get(t.id);
      await waitFor(async () => {
        cur = await client.get(t.id);
        return cur?.status === "queued" && cur.attempt >= 1;
      });
      return cur;
    });

    expect(task?.status).toBe("queued");
    expect(task?.attempt).toBe(1);
    expect(task!.run_at_ms).toBeGreaterThan(Date.now() + 100);
  });
});
