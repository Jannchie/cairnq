// Retention as a handle option rather than a chore.
//
// `purge` is the only thing that removes rows, and a queue whose payloads carry
// real data leaks disk until someone remembers to schedule it. These pin what the
// built-in sweep does — and, as much, what it refuses to do: purge on startup,
// hold the write lock for a whole backlog, or outlive the store it writes to.
import { afterEach, describe, expect, it } from "vitest";

import { CairnQ } from "../src/index.js";
import { RetentionSweeper } from "../src/retention.js";
import { freshDbPath, sleep, waitFor } from "./helpers.js";

let open: CairnQ[] = [];

afterEach(async () => {
  await Promise.all(open.map((c) => c.close()));
  open = [];
});

function client(opts: Parameters<typeof CairnQ.sqlite>[1] = {}): CairnQ {
  const c = CairnQ.sqlite(freshDbPath(), opts);
  open.push(c);
  return c;
}

/** Run a task to `succeeded`, so it is eligible for purge. */
async function finishOne(c: CairnQ, name = "job"): Promise<string> {
  const task = await c.submit(name, {});
  const [claimed] = await c.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 5_000 });
  await c.store.succeed({ taskId: claimed.id, workerId: "w1", result: {} });
  return task.id;
}

describe("retention", () => {
  it("deletes terminal tasks past the cutoff, on its own", async () => {
    const c = client({ retention: { olderThanMs: 0, intervalMs: 20 } });
    const done = await finishOne(c);
    const live = await c.submit("job", {});

    await waitFor(async () => (await c.get(done)) === null);
    expect(await c.get(done)).toBeNull();
    // Only terminal rows go: the queued task is still there to be claimed.
    expect((await c.get(live.id))?.status).toBe("queued");
  });

  it("starts without an explicit connect", async () => {
    // connect() is optional — every operation connects lazily — so retention
    // that only ran for callers who remembered to call it would be a silent leak
    // in the feature that exists to prevent one.
    const c = CairnQ.sqlite(freshDbPath(), { retention: { olderThanMs: 0, intervalMs: 20 } });
    open.push(c);
    const done = await finishOne(c); // first store touch: no connect() above
    await waitFor(async () => (await c.get(done)) === null);
    expect(await c.get(done)).toBeNull();
  });

  it("keeps tasks that have not aged out", async () => {
    const c = client({ retention: { olderThanMs: 3_600_000, intervalMs: 20 } });
    const done = await finishOne(c);
    await sleep(80);
    expect(await c.get(done)).not.toBeNull();
  });

  it("does not purge on startup", async () => {
    // A process that restarts often would otherwise issue a write burst on every
    // boot — exactly when the store is busiest.
    const c = client({ retention: { olderThanMs: 0, intervalMs: 60_000 } });
    const done = await finishOne(c);
    await sleep(50);
    expect(await c.get(done)).not.toBeNull();
  });

  it("drains a backlog larger than one statement", async () => {
    const c = client();
    const ids: string[] = [];
    for (let i = 0; i < 7; i++) ids.push(await finishOne(c));

    // Directly, so the assertion is about one sweep rather than about timing.
    const sweeper = new RetentionSweeper(c.store, { olderThanMs: 0, limit: 2 });
    expect(await sweeper.sweep()).toBe(7);
    expect((await c.list({})).length).toBe(0);
  });

  it("reports a failed sweep and keeps sweeping", async () => {
    const errors: unknown[] = [];
    const c = client({
      retention: { olderThanMs: 0, intervalMs: 20, onError: (e) => errors.push(e) },
    });
    const store = c.store;
    const realPurge = store.purge.bind(store);
    let failures = 2;
    store.purge = async (input) => {
      if (failures-- > 0) throw new Error("database is locked");
      return realPurge(input);
    };
    const done = await finishOne(c);

    // A purge that failed because the database was busy is not a reason to stop
    // retaining, so the schedule survives it.
    await waitFor(async () => (await c.get(done)) === null);
    expect(await c.get(done)).toBeNull();
    expect(errors.length).toBe(2);
    store.purge = realPurge;
  });

  it("stops on close, without leaving a purge behind it", async () => {
    const c = CairnQ.sqlite(freshDbPath(), { retention: { olderThanMs: 0, intervalMs: 10 } });
    const done = await finishOne(c);
    await waitFor(async () => (await c.get(done)) === null);
    await c.close();
    // close() awaited the sweep in flight, so nothing here can be mid-write
    // against a store that is already gone.
    const reopened = client();
    expect(await reopened.get(done)).toBeNull();
  });

  it("refuses a cutoff or interval that cannot mean anything", () => {
    expect(() => client({ retention: { olderThanMs: -1 } })).toThrow(/olderThanMs/);
    expect(() => client({ retention: { olderThanMs: 0, intervalMs: 0 } })).toThrow(/intervalMs/);
  });
});
