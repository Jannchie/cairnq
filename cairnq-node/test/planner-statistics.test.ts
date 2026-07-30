// Keeping sqlite_stat1 current, at open and as a connection ages.
//
// Without statistics SQLite misreads `status = 'running'` as a large fraction of
// the table and passes over the partial cairnq_tasks_lease_idx that lease recovery
// is indexed for — so migration 0004's index and the store's `PRAGMA optimize` are
// one feature, not two. Nothing else fails when either half rots: lease recovery
// just quietly goes back to scanning every running task.
import { describe, expect, it, vi } from "vitest";
import Database from "better-sqlite3";

import { CairnQ } from "../src/index.js";
import { loadStatements } from "../src/sql.js";
import { freshDbPath } from "./helpers.js";

/** Rows sqlite_stat1 currently credits to an index — 0 when it has no entry. */
function analyzedRows(path: string, idx: string): number {
  const db = new Database(path);
  try {
    const row = db.prepare("select stat from sqlite_stat1 where idx = ?").get(idx) as
      | { stat: string }
      | undefined;
    return row ? Number(row.stat.split(" ")[0]) : 0;
  } finally {
    db.close();
  }
}

/** Write `n` tasks from another connection, as another process would. */
function seed(path: string, n: number, status: string): void {
  const db = new Database(path);
  const ins = db.prepare(
    "insert into cairnq_tasks (id,name,queue,status,payload,run_at_ms," +
      "lease_until_ms,created_at_ms,updated_at_ms,completed_at_ms) " +
      `values (?,'job','default','${status}','{}',?,?,?,?,?)`,
  );
  db.transaction(() => {
    for (let i = 0; i < n; i++) {
      const lease = status === "running" ? 2 ** 42 + i : null;
      ins.run(`${status}_${i}`, i, lease, i, i, status === "running" ? null : i);
    }
  })();
  db.close();
}

describe("planner statistics", () => {
  it("analyzes on open, so recover_leases uses its partial index", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    await client.close();

    // The shape the index is designed for, and the reason it beats a plain scan of
    // 'running': a large drift of terminal rows (lease null — see PROTOCOL.md
    // §Lease model) over a handful of live leases bounded by worker concurrency.
    seed(path, 2_000, "succeeded");
    seed(path, 8, "running");

    const reopened = CairnQ.sqlite(path);
    await reopened.connect();
    await reopened.close();

    expect(analyzedRows(path, "cairnq_tasks_lease_idx")).toBeGreaterThan(0);

    const db = new Database(path);
    try {
      const plan = db
        .prepare(`explain query plan ${loadStatements("sqlite").recover_leases}`)
        .all({ now_ms: 0, lease_expired_error: "{}" }) as { detail: string }[];
      expect(plan.map((r) => r.detail).join("\n")).toContain("cairnq_tasks_lease_idx");
    } finally {
      db.close();
    }
  });

  // The case the open-time analyze cannot reach: a worker that started against an
  // empty database and is still running once the backlog is real. Its statements
  // were prepared against the empty shape, and it never reopens.
  it("re-analyzes a connection that outlived its statistics", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    try {
      seed(path, 5_000, "queued");
      const idx = "cairnq_tasks_claim_idx";

      // Still inside the interval: the throttle holds, or this would analyze on
      // every operation — 15ms of write lock per call at a few hundred thousand rows.
      await client.get("absent");
      expect(analyzedRows(path, idx)).toBeLessThan(100);

      vi.useFakeTimers({ toFake: ["Date"] });
      try {
        vi.setSystemTime(Date.now() + 61_000);
        await client.get("absent");
      } finally {
        vi.useRealTimers();
      }

      expect(analyzedRows(path, idx)).toBeGreaterThanOrEqual(5_000);
    } finally {
      await client.close();
    }
  });
});
