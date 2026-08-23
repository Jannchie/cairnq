// Retention as a handle option rather than a chore.
//
// `purge` is the only thing that removes rows, and a queue whose payloads carry
// real data leaks disk until someone remembers to schedule it. These pin what the
// built-in sweep does — and, as much, what it refuses to do: purge on startup,
// hold the write lock for a whole backlog, or outlive the store it writes to.
import { describe, expect, it } from "vitest";

import { CairnQ } from "../src/index.js";
import { RetentionSweeper } from "../src/retention.js";
import type { TaskStore } from "../src/store/base.js";
import { describeBackends } from "./backends.js";
import { failOne, finishOne, freshDbPath, sleep, waitFor } from "./helpers.js";

// Both dialects: purge is the statement whose optional filters `specialize`
// rewrites, and the two dialects reach an index by different routes — SQLite
// needs the rewrite because it plans before the parameters have values, Postgres
// re-plans and folds the branch itself. A sweep that quietly matched nothing on
// one of them would look exactly like a sweep with nothing to do.
describeBackends("retention", (backend) => {
  const client = (opts: Parameters<typeof backend.client>[0] = {}) => backend.client(opts);

  // The handle's own sweeper runs on the default hourly interval, so the
  // timer-driven cases drive a RetentionSweeper directly against the same store.
  // What the handle adds over that is wiring, and the startup case below is
  // where that wiring is observable.
  it("deletes terminal tasks past the cutoff, on its own", async () => {
    const c = await client();
    const done = await finishOne(c);
    const live = await c.submit("job", {});

    const sweeper = new RetentionSweeper(c.store, 0, { intervalMs: 20 });
    sweeper.start();
    try {
      await waitFor(async () => (await c.get(done)) === null);
      expect(await c.get(done)).toBeNull();
      // Only terminal rows go: the queued task is still there to be claimed.
      expect((await c.get(live.id))?.status).toBe("queued");
    } finally {
      await sweeper.stop();
    }
  });

  // Builds its own handle rather than taking one from the fixture: the fixture
  // connects, and connecting is the thing under test. SQLite-only for the same
  // reason — lazy connect is a TaskStore property, not a dialect one, and a
  // Postgres arm would only re-prove it against a slower backend.
  it.runIf(backend.name === "sqlite")("starts without an explicit connect", async () => {
    // connect() is optional — every operation connects lazily — so retention
    // that only ran for callers who remembered to call it would be a silent leak
    // in the feature that exists to prevent one.
    const c = CairnQ.sqlite(freshDbPath());
    const sweeper = new RetentionSweeper(c.store, 0, { intervalMs: 20 });
    sweeper.start(); // sweeping a store nothing has connected yet
    try {
      const done = await finishOne(c);
      await waitFor(async () => (await c.get(done)) === null);
      expect(await c.get(done)).toBeNull();
    } finally {
      await sweeper.stop();
      await c.close();
    }
  });

  it("keeps tasks that have not aged out", async () => {
    const c = await client();
    const done = await finishOne(c);
    const sweeper = new RetentionSweeper(c.store, 3_600_000, { intervalMs: 20 });
    sweeper.start();
    try {
      await sleep(80);
      expect(await c.get(done)).not.toBeNull();
    } finally {
      await sweeper.stop();
    }
  });

  it("does not purge on startup", async () => {
    // A process that restarts often would otherwise issue a write burst on every
    // boot — exactly when the store is busiest. Through the handle, since this is
    // the one thing the handle's own sweeper does at construction time.
    const c = await client({ retentionMs: 0 });
    const done = await finishOne(c);
    await sleep(50);
    expect(await c.get(done)).not.toBeNull();
  });

  it("stops without leaving a purge behind it", async () => {
    const c = await client();
    const done = await finishOne(c);
    const sweeper = new RetentionSweeper(c.store, 0, { intervalMs: 10 });
    sweeper.start();
    await waitFor(async () => (await c.get(done)) === null);
    // stop() awaited the sweep in flight, so nothing after this can be mid-write
    // against a store that is already gone.
    await sweeper.stop();
    await c.close();

    const reopened = await client();
    expect(await reopened.get(done)).toBeNull();
  });

  it("keeps sweeping after a sweep that threw", async () => {
    const c = await client();
    const store = c.store;
    const realPurge = store.purge.bind(store);
    let failures = 2;
    store.purge = async (input) => {
      if (failures-- > 0) throw new Error("database is locked");
      return realPurge(input);
    };
    const done = await finishOne(c);

    const sweeper = new RetentionSweeper(store, 0, { intervalMs: 20 });
    sweeper.start();
    try {
      // A purge that failed because the database was busy is not a reason to
      // stop retaining, so the schedule survives it.
      await waitFor(async () => (await c.get(done)) === null);
      expect(await c.get(done)).toBeNull();
      expect(failures).toBeLessThan(0);
    } finally {
      await sweeper.stop();
      store.purge = realPurge;
    }
  });

  it("refuses a cutoff or interval that cannot mean anything", async () => {
    await expect(client({ retentionMs: -1 })).rejects.toThrow(/retentionMs/);
    const c = await client();
    expect(() => new RetentionSweeper(c.store, 0, { intervalMs: 0 })).toThrow(/intervalMs/);
  });
});

/** The sweeper's fixed rows-per-statement limit. */
const FULL_BATCH = 1_000;

/**
 * A store whose `purge` hands back a scripted sequence of batch sizes, then
 * nothing.
 *
 * The drain loop keeps going while a batch comes back full, and "full" is now a
 * fixed 1000 — so a real backlog spanning more than one statement would be 1001
 * seeded rows per test. The loop reads nothing but how many ids came back, so
 * scripting the batches exercises the same code in milliseconds.
 */
function scriptedStore(...sizes: number[]): TaskStore {
  let call = 0;
  return {
    purge: async () => {
      const n = sizes[call] ?? 0;
      call++;
      return Array.from({ length: n }, (_, i) => `t${call}-${i}`);
    },
  } as unknown as TaskStore;
}

// Dialect-free: every one of these is about the drain loop's own bookkeeping,
// which never reaches SQL.
describe("retention drains", () => {
  it("drains a backlog larger than one statement", async () => {
    const sweeper = new RetentionSweeper(scriptedStore(FULL_BATCH, FULL_BATCH, 7), 0);
    expect(await sweeper.sweep()).toBe(2 * FULL_BATCH + 7);
  });

  it("stops promptly after an on-demand sweep, not an interval later", async () => {
    // sweep() is public and documented as the way to drain on demand, and its
    // between-batches yield is a sleep of its own. When those sleeps shared one
    // slot with the scheduled loop's, the manual one overwrote and then cleared
    // it — stop() had nothing left to wake and close() blocked for the whole
    // interval. An hour, at the default.
    const sweeper = new RetentionSweeper(scriptedStore(FULL_BATCH), 0, {
      intervalMs: 3_600_000,
    });
    sweeper.start();
    await sweeper.sweep();

    const startedAt = Date.now();
    await sweeper.stop();
    expect(Date.now() - startedAt).toBeLessThan(1_000);
  });

  it("still drains on demand after it has been stopped", async () => {
    // sweep() is documented as a direct call — "after a backfill, or from a
    // maintenance command" — and stop() used to leave it crippled. `stopping` is
    // how a sweep in flight cuts itself short, and only start() ever cleared it,
    // so a later sweep() returned after its FIRST batch, reporting success and
    // leaving the rest behind with no indication anything had been skipped.
    const sweeper = new RetentionSweeper(scriptedStore(FULL_BATCH, 5), 0, { intervalMs: 20 });
    sweeper.start();
    await sweeper.stop();

    expect(await sweeper.sweep()).toBe(FULL_BATCH + 5);
  });

  it("holds the process open for the length of an on-demand drain", async () => {
    // The between-batches yield used to be unref'd, like the scheduled loop's
    // interval. For the loop that is right — retention is housekeeping and must
    // never be why a process refuses to exit — but sweep() is a call somebody is
    // AWAITING, and an unref'd timer let Node decide the loop was idle and exit
    // mid-drain, leaving the promise unsettled: a maintenance command that swept
    // two rows of seven and exited 13. Asserted through the timer's own flag,
    // since a test runner keeps the loop alive and would hide the difference.
    const sweeper = new RetentionSweeper(scriptedStore(FULL_BATCH, 2), 0);

    // Only the drain's own 0ms yields are watched, and only whether each of
    // those was unref'd — other timers in flight say nothing about this.
    const yields: { unrefd: boolean }[] = [];
    const realSetTimeout = globalThis.setTimeout;
    globalThis.setTimeout = ((fn: () => void, ms?: number) => {
      const timer = realSetTimeout(fn, ms);
      if (ms === 0) {
        const seen = { unrefd: false };
        yields.push(seen);
        const unref = timer.unref.bind(timer);
        timer.unref = () => {
          seen.unrefd = true;
          return unref();
        };
      }
      return timer;
    }) as typeof setTimeout;
    try {
      expect(await sweeper.sweep()).toBe(FULL_BATCH + 2);
    } finally {
      globalThis.setTimeout = realSetTimeout;
    }
    expect(yields.length).toBeGreaterThan(0);
    expect(yields.some((y) => y.unrefd)).toBe(false);
  });

  it("hands the loop back between batches of a drain after a stop", async () => {
    // The subtler half of the same bug: stop() resolved the shared stop signal,
    // and sweep()'s between-batches yield races that signal. Left spent, the
    // yield resolved on a microtask instead of on a timer, so a long drain never
    // handed the event loop back — starving exactly the submits and claims it is
    // there to protect. Asserted by racing the drain against a macrotask: a real
    // yield lets the timer fire somewhere in the middle.
    const sweeper = new RetentionSweeper(scriptedStore(FULL_BATCH, 2), 0, { intervalMs: 20 });
    sweeper.start();
    await sweeper.stop();

    let ticked = false;
    setTimeout(() => (ticked = true), 0);
    await sweeper.sweep();
    expect(ticked).toBe(true);
  });
});

// Both dialects, for the same reason as the sweeper above — and more directly:
// these ARE the optional filters `specialize` rewrites, one per test.
describeBackends("purge filters", (backend) => {
  const client = (opts: Parameters<typeof backend.client>[0] = {}) => backend.client(opts);

  it("deletes only rows matching a status filter", async () => {
    const c = await client();
    const done = await finishOne(c);
    const failed = await failOne(c);
    await sleep(10); // purge deletes strictly-older rows; same-ms completion would miss

    expect(await c.purge({ status: "succeeded" })).toEqual([done]);
    expect((await c.get(failed))?.status).toBe("failed");
  });

  it("deletes only rows matching a queue filter", async () => {
    const c = await client();
    const rpc = await finishOne(c, { queue: "rpc" });
    const job = await finishOne(c, { queue: "jobs" });
    await sleep(10); // purge deletes strictly-older rows; same-ms completion would miss

    expect(await c.purge({ queue: "rpc" })).toEqual([rpc]);
    expect((await c.get(job))?.status).toBe("succeeded");
  });

  it("deletes only rows matching a name filter", async () => {
    const c = await client();
    const alpha = await finishOne(c, { name: "alpha" });
    const beta = await finishOne(c, { name: "beta" });
    await sleep(10); // purge deletes strictly-older rows; same-ms completion would miss

    expect(await c.purge({ name: "alpha" })).toEqual([alpha]);
    expect((await c.get(beta))?.status).toBe("succeeded");
  });

  it("refuses a status filter that could never match", async () => {
    const c = await client();
    await expect(c.purge({ status: "queued" })).rejects.toThrow(/terminal/);
  });

});
