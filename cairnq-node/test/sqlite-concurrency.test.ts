// Several SQLiteStore handles on one database file, inside one process.
//
// better-sqlite3 is synchronous, and a transaction holds SQLite's write lock
// across `await`s (the seam is shared with Postgres, so the callback is async).
// A second connection in this process then blocks the only thread waiting for
// that lock — and the holder can never reach COMMIT, because reaching it needs
// the thread the waiter is sitting on. busy_timeout cannot break that inversion:
// it is one thread, so the wait just burns the timeout and throws "database is
// locked". The two must not overlap in the first place.
//
// This is the API-and-worker-in-one-process deployment, and it is why the lock
// belongs to the database file rather than to the store object.
import { describe, expect, it } from "vitest";

import { CairnQ } from "../src/index.js";
import { freshDbPath } from "./helpers.js";

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
