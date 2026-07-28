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
  it("doubles per attempt and caps", () => {
    expect(retryDelayMs(1, 1_000, 30_000)).toBe(1_000);
    expect(retryDelayMs(2, 1_000, 30_000)).toBe(2_000);
    expect(retryDelayMs(3, 1_000, 30_000)).toBe(4_000);
    expect(retryDelayMs(20, 1_000, 30_000)).toBe(30_000);
  });

  it("treats a zero base as backoff disabled", () => {
    expect(retryDelayMs(3, 0, 30_000)).toBe(0);
  });

  it("requeues a failed attempt into the future", async () => {
    const worker = Worker.sqlite(dbPath, {
      queues: ["default"],
      pollIntervalMs: 20,
      retryBackoffMs: 500,
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
