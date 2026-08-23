/**
 * Resources: a call ceiling several names draw from.
 *
 * The worker's own `concurrency` caps calls in total, which cannot express the
 * constraint that actually binds a worker doing heavy local work — several
 * *different* handlers contending for one scarce thing (a GPU, an index with a
 * single writer). A resource is a ceiling with more than one name drawing on
 * it; at capacity 1 it is mutual exclusion across those names. A name that only
 * needs to cap itself declares a resource of its own.
 *
 * The gate is at claim, not inside the handler: a semaphore around the body
 * would let the task be claimed first, so it would hold a lease, burn a
 * concurrency slot and heartbeat while waiting its turn.
 *
 * The Python twin (tests/test_resources.py) asserts the same behaviors.
 */

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, Worker } from "../src/index.js";
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

async function drain(ids: string[], worker: Worker) {
  await worker.background(async () => {
    await waitFor(() => allTerminal(client, ids), 5_000);
  });
  const out = new Map<string, Task>();
  for (const id of ids) out.set(id, (await client.get(id))!);
  return out;
}

async function submitMany(name: string, n: number) {
  const ids: string[] = [];
  for (let i = 0; i < n; i++) ids.push((await client.submit(name, {})).id);
  return ids;
}

describe("resources", () => {
  it("excludes calls across names at capacity 1", async () => {
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      resources: { gpu: 1 },
    });
    let live = 0;
    let peak = 0;
    const seen = new Set<string>();

    const hold = async (name: string) => {
      live++;
      peak = Math.max(peak, live);
      seen.add(name);
      await sleep(30);
      live--;
    };

    worker.task("render", { resource: "gpu" }, async () => hold("render"));
    worker.task("compare", { resource: "gpu" }, async () => hold("compare"));

    const ids = [...(await submitMany("render", 4)), ...(await submitMany("compare", 4))];
    const tasks = await drain(ids, worker);

    expect(peak, `gpu capacity is 1 but ${peak} calls held it at once`).toBe(1);
    expect([...seen].sort()).toEqual(["compare", "render"]);
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
  });

  it("treats capacity above one as a shared budget", async () => {
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 6,
      resources: { gpu: 2 },
    });
    let live = 0;
    let peak = 0;

    const hold = async () => {
      live++;
      peak = Math.max(peak, live);
      await sleep(30);
      live--;
    };

    for (const name of ["render", "compare", "segment"]) {
      worker.task(name, { resource: "gpu" }, hold);
    }

    const ids: string[] = [];
    for (const name of ["render", "compare", "segment"]) ids.push(...(await submitMany(name, 3)));
    const tasks = await drain(ids, worker);

    expect(peak, `gpu capacity is 2 but the peak was ${peak}`).toBe(2);
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
  });

  it("does not bound names outside it", async () => {
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      resources: { gpu: 1 },
    });
    let freeLive = 0;
    let freePeak = 0;

    worker.task("render", { resource: "gpu" }, async () => {
      await sleep(50);
    });
    worker.task("thumbnail", async () => {
      freeLive++;
      freePeak = Math.max(freePeak, freeLive);
      await sleep(30);
      freeLive--;
    });

    const ids = [...(await submitMany("render", 2)), ...(await submitMany("thumbnail", 6))];
    const tasks = await drain(ids, worker);

    expect(freePeak, "an unrelated name was held back by someone else's resource").toBeGreaterThan(
      1,
    );
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
  });

  it("cannot overshoot within one poll", async () => {
    // Regression: the in-flight count only moves when a call is dispatched,
    // which happens after the whole plan returns. Without the plan's own tally,
    // two names sharing a resource each see its full ceiling in the same poll
    // and together exceed it. Both names have work queued before the worker
    // starts, so one poll draws for both.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 8,
      resources: { gpu: 1 },
    });
    let live = 0;
    let peak = 0;

    const hold = async () => {
      live++;
      peak = Math.max(peak, live);
      await sleep(40);
      live--;
    };

    worker.task("render", { resource: "gpu" }, hold);
    worker.task("compare", { resource: "gpu" }, hold);

    const ids = [...(await submitMany("render", 3)), ...(await submitMany("compare", 3))];
    const tasks = await drain(ids, worker);

    expect(peak, `one poll drew ${peak} calls against a capacity of 1`).toBe(1);
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
  });

  it("composes with batching", async () => {
    // Members may batch differently — which is why a resource is a ceiling
    // several sources draw down, not one source spanning their names: a source
    // carries a single batch size and could not hold both.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      resources: { gpu: 1 },
    });
    let live = 0;
    let peak = 0;
    let widest = 0;

    worker.task("embed", { batch: 4, resource: "gpu" }, async (items) => {
      live++;
      peak = Math.max(peak, live);
      widest = Math.max(widest, items.length);
      await sleep(30);
      live--;
    });
    worker.task("render", { resource: "gpu" }, async () => {
      live++;
      peak = Math.max(peak, live);
      await sleep(30);
      live--;
    });

    const ids = [...(await submitMany("embed", 8)), ...(await submitMany("render", 2))];
    const tasks = await drain(ids, worker);

    expect(peak, `gpu capacity is 1 but ${peak} calls held it at once`).toBe(1);
    expect(widest, "the batched member did not fill its batch").toBe(4);
    expect([...tasks.values()].every((t) => t.status === "succeeded")).toBe(true);
  });

  it("caps a single name against itself with a resource of its own", async () => {
    // The migration path for a per-name limit: one name, one resource, capacity
    // N. The worker budget allows 6 calls; `embed` may only ever run 2 of them,
    // so one expensive name cannot take the whole worker.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 6,
      resources: { embed: 2 },
    });
    let live = 0;
    let peak = 0;

    worker.task("embed", { batch: 2, resource: "embed" }, async () => {
      live++;
      peak = Math.max(peak, live);
      await sleep(30);
      live--;
    });

    const ids = await submitMany("embed", 20);
    const tasks = await drain(ids, worker);

    expect(peak, `embed is capped at 2 but ${peak} calls ran at once`).toBeLessThanOrEqual(2);
    expect([...tasks.values()].map((t) => t.status)).toEqual(Array(20).fill("succeeded"));
  });

  it("gives the units back when a call fails", async () => {
    // A resource is refunded on every way out of a call, not just the happy one
    // — otherwise a failing name leaks its capacity and wedges the rest.
    const worker = Worker.sqlite(dbPath, {
      pollIntervalMs: 20,
      concurrency: 4,
      retryBackoffMs: 0,
      resources: { gpu: 1 },
    });
    let ran = 0;

    worker.task("boom", { resource: "gpu" }, async () => {
      throw new Error("nope");
    });
    worker.task("after", { resource: "gpu" }, async () => {
      ran++;
    });

    const ids: string[] = [];
    for (let i = 0; i < 3; i++) ids.push((await client.submit("boom", {}, { maxAttempts: 1 })).id);
    ids.push(...(await submitMany("after", 3)));
    const tasks = await drain(ids, worker);

    expect(ran, "the failing name leaked its resource units").toBe(3);
    for (const t of tasks.values()) {
      expect(t.status).toBe(t.name === "boom" ? "failed" : "succeeded");
    }
  });

  it("rejects an undeclared resource at registration", () => {
    // A typo would otherwise read as an unbounded resource — silently removing
    // the ceiling the caller asked for, which is the whole point of the option.
    const worker = Worker.sqlite(dbPath, { resources: { gpu: 1 } });
    expect(() => worker.task("render", { resource: "gpü" }, async () => {})).toThrow(/gpu/);
  });

  it("rejects a capacity below one", () => {
    expect(() => Worker.sqlite(dbPath, { resources: { gpu: 0 } })).toThrow(/>= 1/);
  });
});
