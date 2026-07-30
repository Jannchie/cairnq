// Migration application. The interesting case is not a fresh database — every
// other test covers that — but an existing one created by an older SDK, which must
// pick up later migrations without re-running the ones it already has.
import { describe, expect, it } from "vitest";
import Database from "better-sqlite3";

import { CairnQ } from "../src/index.js";
import { nowMs } from "../src/ids.js";
import { loadMigrations, loadStatements } from "../src/sql.js";
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
    const names = indexNames(path);
    expect(names.has("cairnq_tasks_completed_idx")).toBe(true);
    expect(names.has("cairnq_tasks_lease_idx")).toBe(true);
    expect(meta(path, "schema_version")).toBe("4");
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
      expect(indexNames(path).has("cairnq_tasks_lease_idx")).toBe(true);
      expect(meta(path, "schema_version")).toBe("4");
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

  // Migration 0004's index and the `PRAGMA optimize` on the open path are one
  // feature, not two — without statistics the planner passes the index over (see
  // 0004_lease_index.sql). So assert the pair, or either half can rot unnoticed:
  // nothing else fails when lease recovery quietly goes back to a full status scan.
  it("analyzes on open, so recover_leases uses its partial index", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    await client.close();

    // Terminal rows keep a stale lease_until_ms (complete.sql does not clear it),
    // which is exactly why the index has to be partial on status.
    const seed = new Database(path);
    const ins = seed.prepare(
      "insert into cairnq_tasks (id,name,queue,status,payload,run_at_ms," +
        "lease_until_ms,created_at_ms,updated_at_ms,completed_at_ms) " +
        "values (?,'job','default','succeeded','{}',?,?,?,?,?)",
    );
    seed.transaction(() => {
      for (let i = 0; i < 2_000; i++) ins.run(`seed_${i}`, i, i + 500, i, i, i);
    })();
    seed.close();

    const reopened = CairnQ.sqlite(path);
    await reopened.connect();
    await reopened.close();

    const db = new Database(path);
    try {
      const analyzed = db
        .prepare("select idx from sqlite_stat1 where idx = 'cairnq_tasks_lease_idx'")
        .all();
      expect(analyzed).toHaveLength(1);

      const plan = db
        .prepare(`explain query plan ${loadStatements("sqlite").recover_leases}`)
        .all({ now_ms: 0, lease_expired_error: "{}" }) as { detail: string }[];
      expect(plan.map((r) => r.detail).join("\n")).toContain("cairnq_tasks_lease_idx");
    } finally {
      db.close();
    }
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
