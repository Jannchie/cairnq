// Claim order is by when a task became DUE (run_at_ms), not by when it was first
// created — and the claim index carries the whole ORDER BY, tie-break included.
//
// The two are one property, and this file pins both halves because dropping
// either brings back the same failure: a queued-but-not-yet-due task (a retry
// waiting out its backoff, a delayed submit) sorted ahead of the tasks that were
// actually claimable, so every draw walked the entire backoff pile inside the
// transaction that holds the claim. That pile is largest exactly when a
// downstream dependency has just failed and thousands of tasks are backing off
// together. See migration 0008.
import { describe, expect, it } from "vitest";
import Database from "better-sqlite3";

import { CairnQ } from "../src/index.js";
import { loadStatements } from "../src/sql.js";
import { freshDbPath } from "./helpers.js";

/** Write tasks from another connection, as another process would. `runAt` is an
 * offset from `now`, so a positive one is a task that is queued but not yet due. */
function seed(
  path: string,
  rows: { id: string; createdAt: number; runAt: number }[],
  now: number,
): void {
  const db = new Database(path);
  const ins = db.prepare(
    "insert into cairnq_tasks (id,name,queue,status,payload,run_at_ms," +
      "created_at_ms,updated_at_ms) values (?,'job','default','queued','{}',?,?,?)",
  );
  db.transaction(() => {
    for (const r of rows) ins.run(r.id, now + r.runAt, now + r.createdAt, now + r.createdAt);
  })();
  db.close();
}

describe("claim order follows run_at_ms", () => {
  // Not a test of the ordering — say so plainly, because the setup looks like
  // one. Which rows a claim TAKES here is the same under either order: the
  // not-yet-due ones are excluded by `run_at_ms <= :now_ms` whatever they sort
  // like, and 0008 changed what that costs, not what it returns. What this does
  // pin is the invariant underneath, which nothing else covered: a task that is
  // queued but not yet due is never handed to a worker, however old it is.
  it("never claims a task that is not due yet, however old it is", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    try {
      const now = Date.now();
      // Two rows say it: the older one is not due, the newer one is. A backlog
      // would only make the setup look like a cost test, and the cost half is
      // the plan assertion below.
      seed(
        path,
        [
          { id: "not_due", createdAt: -100_000, runAt: 600_000 },
          { id: "due", createdAt: 0, runAt: -1 },
        ],
        now,
      );

      const claimed = await client.store.claim({
        queues: ["default"],
        workerId: "w1",
        leaseMs: 5_000,
        limit: 5,
      });
      expect(claimed.map((t) => t.id)).toEqual(["due"]);
      expect((await client.get("not_due"))!.status).toBe("queued");
    } finally {
      await client.close();
    }
  });

  it("does not let a delayed task cut ahead of what was queued while it waited", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    try {
      const now = Date.now();
      // `deferred` was created first but became due last — the shape of a task
      // that failed, backed off, and came back to a queue that filled up
      // meanwhile. Ordering by creation would hand it back first.
      seed(
        path,
        [
          { id: "deferred", createdAt: -10_000, runAt: -1 },
          { id: "queued_meanwhile", createdAt: -5_000, runAt: -5_000 },
        ],
        now,
      );

      // One at a time: claim order decides which rows are taken, not what order
      // they come back in — `returning *` follows the UPDATE's own visit order.
      const first = await client.store.claim({ queues: ["default"], workerId: "w1" });
      const second = await client.store.claim({ queues: ["default"], workerId: "w1" });
      expect([first[0]?.id, second[0]?.id]).toEqual(["queued_meanwhile", "deferred"]);
    } finally {
      await client.close();
    }
  });

  // The plan is the actual fix, and it is invisible from behavior: a sort node
  // makes the scan run to completion before it can emit anything, so LIMIT stops
  // nothing and the walk above comes back however the rows are ordered.
  it("reads the claim index for the whole ORDER BY, with no sorter", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    await client.close();

    const db = new Database(path);
    try {
      for (const name of ["claim_one_queue", "claim_one_queue_one_name"] as const) {
        const plan = db
          .prepare(`explain query plan ${loadStatements("sqlite")[name]}`)
          .all({
            queue: "default",
            name: "job",
            names: null,
            now_ms: 0,
            worker_id: "w1",
            lease_until_ms: 0,
            limit: 1,
          }) as { detail: string }[];
        const detail = plan.map((r) => r.detail).join("\n");
        expect(detail).toContain("cairnq_tasks_claim_");
        expect(detail).not.toContain("TEMP B-TREE");
      }
    } finally {
      db.close();
    }
  });
});
