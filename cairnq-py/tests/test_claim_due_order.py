"""Claim order is by when a task became DUE (run_at_ms), not by when it was first
created — and the claim index carries the whole ORDER BY, tie-break included.

The two are one property, and this file pins both halves because dropping either
brings back the same failure: a queued-but-not-yet-due task (a retry waiting out
its backoff, a delayed submit) sorted ahead of the tasks that were actually
claimable, so every draw walked the entire backoff pile inside the transaction
that holds the claim. That pile is largest exactly when a downstream dependency
has just failed and thousands of tasks are backing off together. See migration
0008. Twin of cairnq-node/test/claim-due-order.test.ts."""

from __future__ import annotations

import sqlite3
import time

from cairnq import CairnQ
from cairnq._sql import load_statements


def seed(path: str, rows: list[tuple[str, int, int]], now: int) -> None:
    """Write tasks from another connection, as another process would. The offsets
    are relative to `now`, so a positive run_at is a task that is queued but not
    yet due."""
    db = sqlite3.connect(path)
    try:
        db.executemany(
            "insert into cairnq_tasks (id,name,queue,status,payload,run_at_ms,"
            "created_at_ms,updated_at_ms) values (?,'job','default','queued','{}',?,?,?)",
            [
                (task_id, now + run_at, now + created_at, now + created_at)
                for task_id, created_at, run_at in rows
            ],
        )
        db.commit()
    finally:
        db.close()


# Not a test of the ordering — say so plainly, because the setup looks like one.
# Which rows a claim TAKES here is the same under either order: the not-yet-due
# ones are excluded by `run_at_ms <= :now_ms` whatever they sort like, and 0008
# changed what that costs, not what it returns. What this does pin is the
# invariant underneath, which nothing else covered: a task that is queued but not
# yet due is never handed to a worker, however old it is.
async def test_never_claims_a_task_that_is_not_due_yet(db_path):
    client = CairnQ.sqlite(db_path)
    await client.connect()
    try:
        now = int(time.time() * 1000)
        # Two rows say it: the older one is not due, the newer one is. A backlog
        # would only make the setup look like a cost test, and the cost half is
        # the plan assertion below.
        seed(db_path, [("not_due", -100_000, 600_000), ("due", 0, -1)], now)

        claimed = await client.store.claim(
            queues=["default"], worker_id="w1", lease_ms=5_000, limit=5
        )
        assert [t.id for t in claimed] == ["due"]
        assert len(await client.list(status="queued")) == 1
    finally:
        await client.close()


async def test_a_delayed_task_does_not_cut_ahead_of_what_was_queued_meanwhile(db_path):
    client = CairnQ.sqlite(db_path)
    await client.connect()
    try:
        now = int(time.time() * 1000)
        # `deferred` was created first but became due last — the shape of a task
        # that failed, backed off, and came back to a queue that filled up
        # meanwhile. Ordering by creation would hand it back first.
        seed(db_path, [("deferred", -10_000, -1), ("queued_meanwhile", -5_000, -5_000)], now)

        # One at a time: claim order decides which rows are taken, not what order
        # they come back in — `returning *` follows the UPDATE's own visit order.
        first = await client.store.claim(queues=["default"], worker_id="w1")
        second = await client.store.claim(queues=["default"], worker_id="w1")
        assert [first[0].id, second[0].id] == ["queued_meanwhile", "deferred"]
    finally:
        await client.close()


async def test_claim_reads_its_index_for_the_whole_order_by_with_no_sorter(db_path):
    # The plan is the actual fix, and it is invisible from behavior: a sort node
    # makes the scan run to completion before it can emit anything, so LIMIT stops
    # nothing and the walk above comes back however the rows are ordered.
    client = CairnQ.sqlite(db_path)
    await client.connect()
    await client.close()

    db = sqlite3.connect(db_path)
    try:
        for name in ("claim_one_queue", "claim_one_queue_one_name"):
            plan = db.execute(
                "explain query plan " + load_statements("sqlite")[name],
                {
                    "queue": "default",
                    "name": "job",
                    "names": None,
                    "now_ms": 0,
                    "worker_id": "w1",
                    "lease_until_ms": 0,
                    "limit": 1,
                },
            ).fetchall()
            detail = "\n".join(row[-1] for row in plan)
            assert "cairnq_tasks_claim_" in detail
            assert "TEMP B-TREE" not in detail
    finally:
        db.close()
