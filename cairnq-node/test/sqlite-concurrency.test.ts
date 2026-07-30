// Several SQLiteStore handles on one database file, inside one process.
//
// better-sqlite3 is synchronous, and a transaction holds SQLite's write lock
// across `await`s (the seam is shared with Postgres, so the callback is async).
// Two connections in this process therefore must not contend for that lock
// directly — they queue behind one per-file lock instead, which is why that lock
// is keyed by database rather than by store object.
//
// This is the API-and-worker-in-one-process deployment.
import { describe, expect, it } from "vitest";
import Database from "better-sqlite3";

import { CairnQ } from "../src/index.js";
import { freshDbPath, sleep } from "./helpers.js";

describe("sqlite concurrency", () => {
  it("lets two handles on one file write concurrently", async () => {
    const dbPath = freshDbPath();
    const a = CairnQ.sqlite(dbPath);
    const b = CairnQ.sqlite(dbPath);
    await a.connect();
    await b.connect();

    try {
      // Keyed submits, so each goes through a transaction rather than a single
      // statement — that is what holds the write lock across an await.
      const submits = [
        ...Array.from({ length: 10 }, (_, i) => a.submit("job", { i }, { key: `a${i}` })),
        ...Array.from({ length: 10 }, (_, i) => b.submit("job", { i }, { key: `b${i}` })),
      ];
      const tasks = await Promise.all(submits);
      expect(new Set(tasks.map((t) => t.id)).size).toBe(20);
    } finally {
      await a.close();
      await b.close();
    }
  });

  // A lost write lock must be waited out by the event loop, not by the thread.
  // A nonzero busy_timeout waits inside the synchronous driver, so a caller that
  // loses the lock freezes this process for up to the whole timeout — and in the
  // cases below it would never get the lock at all, because the timer that
  // releases it needs the thread the wait is sitting on.
  //
  // The blocker is a plain connection, not a store: nothing serializes it against
  // the store, which is what a competing *process* looks like from here.
  for (const keyed of [false, true]) {
    it(`waits out a contended write lock without blocking the event loop (${
      keyed ? "transaction" : "single statement"
    })`, async () => {
      const dbPath = freshDbPath();
      const client = CairnQ.sqlite(dbPath);
      await client.connect();

      const blocker = new Database(dbPath);
      blocker.pragma("busy_timeout = 0");
      blocker.exec("BEGIN IMMEDIATE");
      const released = sleep(300).then(() => {
        blocker.exec("COMMIT");
        blocker.close();
      });

      let ticks = 0;
      const ticker = setInterval(() => ticks++, 10);
      const started = Date.now();
      try {
        const task = await client.submit("job", { v: 1 }, keyed ? { key: "k" } : {});
        const elapsed = Date.now() - started;

        // It really did have to wait for the blocker...
        expect(elapsed).toBeGreaterThan(250);
        // ...and the event loop ran throughout, which is also the only reason the
        // COMMIT above ever happened. ~30 ticks are due in 300ms; anything well
        // clear of zero proves the thread was free.
        expect(ticks).toBeGreaterThan(10);
        expect((await client.get(task.id))?.payload).toEqual({ v: 1 });
      } finally {
        clearInterval(ticker);
        await released;
        await client.close();
      }
    });
  }

  it("connects to an in-memory database", async () => {
    // WAL is a property of an on-disk file; an in-memory database reports
    // journal_mode = "memory" and can never become WAL. Waiting for it to turn
    // WAL anyway burns the whole retry budget and then blames a lock that was
    // never there.
    const c = CairnQ.sqlite(":memory:");
    await c.connect();
    try {
      const t = await c.submit("job", { n: 1 });
      expect((await c.get(t.id))?.status).toBe("queued");
    } finally {
      await c.close();
    }
  });

  it("keeps :memory: handles independent", async () => {
    // Two :memory: databases share a path but not a database, so they must not
    // share the file lock either — otherwise unrelated stores serialize forever.
    const a = CairnQ.sqlite(":memory:");
    const b = CairnQ.sqlite(":memory:");
    await a.connect();
    await b.connect();
    try {
      await Promise.all([a.submit("job", {}, { key: "k" }), b.submit("job", {}, { key: "k" })]);
      expect(await a.getByKey("k")).not.toBeNull();
      expect(await b.getByKey("k")).not.toBeNull();
    } finally {
      await a.close();
      await b.close();
    }
  });
});
