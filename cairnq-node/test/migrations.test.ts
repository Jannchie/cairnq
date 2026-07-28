// Migration application. The interesting case is not a fresh database — every
// other test covers that — but an existing one created by an older SDK, which must
// pick up later migrations without re-running the ones it already has.
import { describe, expect, it } from "vitest";
import Database from "better-sqlite3";

import { CairnQ } from "../src/index.js";
import { nowMs } from "../src/ids.js";
import { loadMigrations } from "../src/sql.js";
import { freshDbPath } from "./helpers.js";

function indexNames(path: string): Set<string> {
  const db = new Database(path);
  try {
    const rows = db
      .prepare("select name from sqlite_master where type='index'")
      .all() as { name: string }[];
    return new Set(rows.map((r) => r.name));
  } finally {
    db.close();
  }
}

function meta(path: string, key: string): string | undefined {
  const db = new Database(path);
  try {
    const row = db.prepare("select value from cairnq_meta where key = ?").get(key) as
      | { value: string }
      | undefined;
    return row?.value;
  } finally {
    db.close();
  }
}

describe("migrations", () => {
  it("applies every migration to a fresh database", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    await client.close();

    const db = new Database(path);
    const applied = new Set(
      (db.prepare("select name from cairnq_migrations").all() as { name: string }[]).map(
        (r) => r.name,
      ),
    );
    db.close();

    expect(applied).toEqual(new Set(loadMigrations("sqlite").map((m) => m.name)));
    expect(indexNames(path).has("cairnq_tasks_completed_idx")).toBe(true);
    expect(meta(path, "schema_version")).toBe("2");
  });

  it("upgrades a database left at an older migration", async () => {
    const path = freshDbPath();
    const [first] = loadMigrations("sqlite");
    const db = new Database(path);
    db.exec(first.sql);
    db.exec(
      "create table if not exists cairnq_migrations " +
        "(name text primary key, applied_at_ms integer not null)",
    );
    db.prepare("insert into cairnq_migrations (name, applied_at_ms) values (?, ?)").run(
      first.name,
      nowMs(),
    );
    db.close();

    expect(indexNames(path).has("cairnq_tasks_completed_idx")).toBe(false);
    expect(meta(path, "schema_version")).toBe("1");

    const client = CairnQ.sqlite(path);
    await client.connect();
    try {
      expect(indexNames(path).has("cairnq_tasks_completed_idx")).toBe(true);
      expect(meta(path, "schema_version")).toBe("2");
      const task = await client.submit("job", { v: 1 });
      expect((await client.get(task.id))?.payload).toEqual({ v: 1 });
    } finally {
      await client.close();
    }
  });

  it("applies each migration once across a concurrent cold start", async () => {
    // Separate stores means separate connections, so they contend on the file
    // exactly as separate processes would. The sequential cases above would pass
    // even without the check-and-apply happening under one write lock.
    const path = freshDbPath();
    const clients = Array.from({ length: 4 }, () => CairnQ.sqlite(path));
    try {
      await Promise.all(clients.map((c) => c.connect()));
    } finally {
      await Promise.all(clients.map((c) => c.close().catch(() => undefined)));
    }

    const db = new Database(path);
    const rows = db
      .prepare("select name, count(*) as n from cairnq_migrations group by name")
      .all() as { name: string; n: number }[];
    db.close();

    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((r) => r.n === 1)).toBe(true);
    expect(indexNames(path).has("cairnq_tasks_completed_idx")).toBe(true);
  });

  it("does not reapply on reopen", async () => {
    const path = freshDbPath();
    for (let i = 0; i < 3; i++) {
      const client = CairnQ.sqlite(path);
      await client.connect();
      await client.close();
    }
    const db = new Database(path);
    const rows = db
      .prepare("select name, count(*) as n from cairnq_migrations group by name")
      .all() as { name: string; n: number }[];
    db.close();
    expect(rows.every((r) => r.n === 1)).toBe(true);
  });
});
