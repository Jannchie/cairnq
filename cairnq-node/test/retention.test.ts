// Retention as a handle option rather than a chore.
//
// `purge` is the only thing that removes rows, and a queue whose payloads carry
// real data leaks disk until someone remembers to schedule it. These pin what the
// built-in sweep does — and, as much, what it refuses to do: purge on startup,
// hold the write lock for a whole backlog, or outlive the store it writes to.
import { expect, it } from "vitest";

import { CairnQ } from "../src/index.js";
import { RetentionSweeper } from "../src/retention.js";
import type { TaskStore } from "../src/store/base.js";
import { describeBackends } from "./backends.js";
import { failOne, finishOne, freshDbPath, sleep, waitFor } from "./helpers.js";

// Both dialects: purge is the statement whose optional filters `specialize`
// rewrites, and the two dialects reach an index by different routes — SQLite
// needs the rewrite because it plans before the parameters have values, Postgres
// re-plans and folds the branch itself. A tiered sweep that quietly matched
// nothing on one of them would look exactly like a sweep with nothing to do.
describeBackends("retention", (backend) => {
  const client = (opts: Parameters<typeof backend.client>[0] = {}) => backend.client(opts);

  it("deletes terminal tasks past the cutoff, on its own", async () => {
    const c = await client({ retention: { olderThanMs: 0, intervalMs: 20 } });
    const done = await finishOne(c);
    const live = await c.submit("job", {});

    await waitFor(async () => (await c.get(done)) === null);
    expect(await c.get(done)).toBeNull();
    // Only terminal rows go: the queued task is still there to be claimed.
    expect((await c.get(live.id))?.status).toBe("queued");
  });

  // Builds its own handle rather than taking one from the fixture: the fixture
  // connects, and connecting is the thing under test. SQLite-only for the same
  // reason — lazy connect is a TaskStore property, not a dialect one, and a
  // Postgres arm would only re-prove it against a slower backend.
  it.runIf(backend.name === "sqlite")("starts without an explicit connect", async () => {
    // connect() is optional — every operation connects lazily — so retention
    // that only ran for callers who remembered to call it would be a silent leak
    // in the feature that exists to prevent one.
    const c = CairnQ.sqlite(freshDbPath(), { retention: { olderThanMs: 0, intervalMs: 20 } });
    const done = await finishOne(c); // first store touch: no connect() above
    await waitFor(async () => (await c.get(done)) === null);
    expect(await c.get(done)).toBeNull();
  });

  it("keeps tasks that have not aged out", async () => {
    const c = await client({ retention: { olderThanMs: 3_600_000, intervalMs: 20 } });
    const done = await finishOne(c);
    await sleep(80);
    expect(await c.get(done)).not.toBeNull();
  });

  it("does not purge on startup", async () => {
    // A process that restarts often would otherwise issue a write burst on every
    // boot — exactly when the store is busiest.
    const c = await client({ retention: { olderThanMs: 0, intervalMs: 60_000 } });
    const done = await finishOne(c);
    await sleep(50);
    expect(await c.get(done)).not.toBeNull();
  });

  it("drains a backlog larger than one statement", async () => {
    const c = await client();
    const ids: string[] = [];
    for (let i = 0; i < 7; i++) ids.push(await finishOne(c));

    // Directly, so the assertion is about one sweep rather than about timing.
    const sweeper = new RetentionSweeper(c.store, { olderThanMs: 0, limit: 2 });
    expect(await sweeper.sweep()).toBe(7);
    expect((await c.list({})).length).toBe(0);
  });

  it("reports a failed sweep and keeps sweeping", async () => {
    const errors: unknown[] = [];
    const c = await client({
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
    const c = await client({ retention: { olderThanMs: 0, intervalMs: 10 } });
    const done = await finishOne(c);
    await waitFor(async () => (await c.get(done)) === null);
    await c.close();
    // close() awaited the sweep in flight, so nothing here can be mid-write
    // against a store that is already gone.
    const reopened = await client();
    expect(await reopened.get(done)).toBeNull();
  });

  it("refuses a cutoff or interval that cannot mean anything", async () => {
    await expect(client({ retention: { olderThanMs: -1 } })).rejects.toThrow(/olderThanMs/);
    await expect(client({ retention: { olderThanMs: 0, intervalMs: 0 } })).rejects.toThrow(/intervalMs/);
  });

  it("keeps each status on its own clock", async () => {
    // Retention needs are tiered: succeeded rows are spent once consumed, failed
    // ones are worth keeping for diagnosis. A status the map does not name is
    // never swept — granularity is an explicit statement of what may go.
    const c = await client({ retention: { olderThanMs: { succeeded: 0 }, intervalMs: 20 } });
    const done = await finishOne(c);
    const failed = await failOne(c);

    await waitFor(async () => (await c.get(done)) === null);
    expect(await c.get(done)).toBeNull();
    expect((await c.get(failed))?.status).toBe("failed");
  });

  it("stops promptly after an on-demand sweep, not an interval later", async () => {
    // sweep() is public and documented as the way to drain on demand, and its
    // between-batches yield is a sleep of its own. When those sleeps shared one
    // slot with the scheduled loop's, the manual one overwrote and then cleared
    // it — stop() had nothing left to wake and close() blocked for the whole
    // interval. An hour, at the default.
    let firstBatch = true;
    const store = {
      purge: async () => {
        // A drain long enough to yield between batches: the loop only sleeps
        // when a batch came back full.
        const ids = firstBatch ? Array.from({ length: 1_000 }, (_, i) => String(i)) : [];
        firstBatch = false;
        return ids;
      },
    } as unknown as TaskStore;

    const sweeper = new RetentionSweeper(store, { olderThanMs: 0, intervalMs: 3_600_000 });
    sweeper.start();
    await sweeper.sweep();

    const startedAt = Date.now();
    await sweeper.stop();
    expect(Date.now() - startedAt).toBeLessThan(1_000);
  });

  it("refuses a per-status map that names nothing, or a live status", async () => {
    await expect(client({ retention: { olderThanMs: {} } })).rejects.toThrow(/at least one rule/);
    await expect(client({ retention: { olderThanMs: { queued: 0 } as never } })).rejects.toThrow(/terminal/);
    await expect(client({ retention: { olderThanMs: { succeeded: -1 } } })).rejects.toThrow(/>= 0/);
  });

  it("refuses a rule array that names nothing, or a rule purge would reject", async () => {
    await expect(client({ retention: { olderThanMs: [] } })).rejects.toThrow(/at least one rule/);
    await expect(client({ retention: { olderThanMs: [{ status: "queued" as never, olderThanMs: 0 }] } })).rejects.toThrow(/terminal/);
    await expect(client({ retention: { olderThanMs: [{ olderThanMs: -1 }] } })).rejects.toThrow(/>= 0/);
  });

  it("sweeps each rule on its own cutoff, so one queue's retention is not the other's", async () => {
    const c = await client();
    const rpc = await finishOne(c, { queue: "rpc" });
    const job = await finishOne(c, { queue: "jobs" });
    const broken = await failOne(c, { queue: "jobs" });
    await sleep(10); // purge deletes strictly-older rows

    const sweeper = new RetentionSweeper(c.store, {
      // The shape the whole feature is for: one installation, two workloads —
      // an RPC result spent on read, a job's failure kept for diagnosis.
      olderThanMs: [
        { queue: "rpc", olderThanMs: 0 },
        { queue: "jobs", status: "failed", olderThanMs: 3_600_000 },
      ],
      intervalMs: 3_600_000,
    });
    expect(await sweeper.sweep()).toBe(1);
    expect(await c.get(rpc)).toBeNull();
    // Neither jobs row matched: the succeeded one has no rule at all, and the
    // failed one has an hour to go.
    expect((await c.get(job))?.status).toBe("succeeded");
    expect((await c.get(broken))?.status).toBe("failed");
  });

  it("still drains on demand after it has been stopped", async () => {
    // sweep() is documented as a direct call — "after a backfill, or from a
    // maintenance command" — and stop() used to leave it crippled. `stopping` is
    // how a sweep in flight cuts itself short, and only start() ever cleared it,
    // so a later sweep() returned after its FIRST batch: with a backlog of 7 and
    // a limit of 2 it deleted 2, reported success, and left 5 rows behind with
    // no indication anything had been skipped.
    const c = await client();
    for (let i = 0; i < 7; i++) await finishOne(c);
    const sweeper = new RetentionSweeper(c.store, { olderThanMs: 0, limit: 2 });
    sweeper.start();
    await sweeper.stop();

    expect(await sweeper.sweep()).toBe(7);
    expect(await c.list()).toEqual([]);
  });

  it("holds the process open for the length of an on-demand drain", async () => {
    // The between-batches yield used to be unref'd, like the scheduled loop's
    // interval. For the loop that is right — retention is housekeeping and must
    // never be why a process refuses to exit — but sweep() is a call somebody is
    // AWAITING, and an unref'd timer let Node decide the loop was idle and exit
    // mid-drain, leaving the promise unsettled: a maintenance command that swept
    // two rows of seven and exited 13. Asserted through the timer's own flag,
    // since a test runner keeps the loop alive and would hide the difference.
    const c = await client();
    for (let i = 0; i < 7; i++) await finishOne(c);
    const sweeper = new RetentionSweeper(c.store, { olderThanMs: 0, limit: 2 });

    // Only the drain's own 0ms yields are watched, and only whether each of
    // those was unref'd — other timers in flight (the store's busy retry) say
    // nothing about this.
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
      expect(await sweeper.sweep()).toBe(7);
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
    const c = await client();
    for (let i = 0; i < 7; i++) await finishOne(c);
    const sweeper = new RetentionSweeper(c.store, { olderThanMs: 0, limit: 2 });
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
