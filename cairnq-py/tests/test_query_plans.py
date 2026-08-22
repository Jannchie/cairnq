"""What the planner actually does with the canonical statements.

The protocol's SQL carries a lot of reasoning about index use — 0008 measured a
claim's ORDER BY being served from the index, recover_leases.sql says in so many
words "do not simplify this away", queue_depth.sql exists to be bounded. All of
that is enforced by comment today: a change that keeps the results correct and
silently loses the index passes every other test in both suites.

These pin the PLAN. They run the real statement text through EXPLAIN QUERY PLAN
against a real migrated database, so a statement that stops using its index fails
here rather than in someone's production.

Two rules of thumb for reading a failure:

- "USE TEMP B-TREE FOR ORDER BY" means the ORDER BY is no longer served by the
  index, so LIMIT can no longer stop early and the cost grows with the backlog.
- A plan naming a different index than the one asserted is usually a predicate
  that stopped being indexable, not a planner whim. The commonest cause is an
  optional `(:p is null or col = :p)` filter, which SQLite must plan for both
  branches because it plans before parameters have values. That is why purge and
  claim both ship equality specializations.

SQLite only: EXPLAIN output is dialect-specific, and the Postgres side has no
server to run against outside CI. The two dialects' statements differ only in
their filters' spelling, so what is pinned here is the shape both were designed
around.
"""

from __future__ import annotations

import sqlite3

import pytest

from cairnq import CairnQ
from cairnq._sql import load_statements

STATEMENTS = load_statements("sqlite")
#: Enough rows, spread over several queues and statuses, that the planner
#: prefers an index to a scan and ANALYZE has something to measure.
ROWS = 4_000


@pytest.fixture(scope="module")
def planned(tmp_path_factory):
    """A migrated database with a realistic spread of rows, and a plain sqlite3
    connection to ask EXPLAIN QUERY PLAN through."""
    import asyncio

    path = str(tmp_path_factory.mktemp("plans") / "t.db")

    async def migrate():
        async with CairnQ.sqlite(path):
            pass

    asyncio.run(migrate())

    conn = sqlite3.connect(path)
    queues = ("default", "rpc", "jobs")
    statuses = ("queued", "running", "succeeded", "failed")
    conn.executemany(
        "insert into cairnq_tasks (id,name,queue,status,payload,priority,run_at_ms,"
        "worker_id,lease_until_ms,attempt,max_attempts,created_at_ms,updated_at_ms,"
        "completed_at_ms) values (?,?,?,?,'{}',0,?,null,?,0,3,?,?,?)",
        [
            (
                f"id{i}",
                f"job{i % 5}",
                queues[i % len(queues)],
                statuses[i % len(statuses)],
                i,
                i if statuses[i % len(statuses)] == "running" else None,
                i,
                i,
                i,
            )
            for i in range(ROWS)
        ],
    )
    conn.commit()
    conn.execute("analyze")
    yield conn
    conn.close()


def plan(conn: sqlite3.Connection, statement: str, params: dict) -> str:
    """The planner's own description of how it will run this statement."""
    sql = STATEMENTS[statement]
    return " | ".join(r[3] for r in conn.execute("explain query plan " + sql, params))


PURGE_BASE = {"before_ms": 10**12, "queue": None, "status": None, "name": None, "limit": 100}

# Which index each purge shape must reach, and with which parameters. The point
# of the specializations is that every filtered shape reads only its own range
# instead of walking the whole retained backlog in completion order.
PURGE_CASES = [
    ("purge", {}, "cairnq_tasks_completed_idx"),
    ("purge_one_status", {"status": "succeeded"}, "cairnq_tasks_status_completed_idx"),
    ("purge_one_queue", {"queue": "rpc"}, "cairnq_tasks_queue_completed_idx"),
    (
        "purge_one_queue_one_status",
        {"queue": "rpc", "status": "succeeded"},
        "cairnq_tasks_queue_completed_idx",
    ),
]


@pytest.mark.parametrize("statement,params,index", PURGE_CASES, ids=[c[0] for c in PURGE_CASES])
def test_purge_reaches_its_index(planned, statement, params, index):
    got = plan(planned, statement, {**PURGE_BASE, **params})
    assert index in got, f"{statement} no longer reads {index}: {got}"
    # The ORDER BY decides WHICH rows a bounded sweep takes, so a sort here means
    # the whole matching set is read to return `limit` of it.
    assert "TEMP B-TREE" not in got, f"{statement} now sorts instead of reading in order: {got}"


def test_claim_one_queue_reads_the_claim_index_in_order(planned):
    """0008's whole subject: the ORDER BY served from cairnq_tasks_claim_idx, so
    a queue deep in backoff does not cost the claim transaction a full sort."""
    got = plan(
        planned,
        "claim_one_queue",
        {"queue": "default", "names": None, "now_ms": 10**12, "limit": 1,
         "lease_until_ms": 10**12, "worker_id": "w1"},
    )
    assert "cairnq_tasks_claim_idx" in got, got
    assert "TEMP B-TREE" not in got, f"claim's ORDER BY is no longer served by the index: {got}"


def test_claim_one_queue_one_name_reads_the_per_name_index(planned):
    got = plan(
        planned,
        "claim_one_queue_one_name",
        {"queue": "default", "name": "job1", "now_ms": 10**12, "limit": 1,
         "lease_until_ms": 10**12, "worker_id": "w1"},
    )
    assert "cairnq_tasks_claim_name_idx" in got, got
    assert "TEMP B-TREE" not in got, got


def test_recover_leases_reads_the_lease_index(planned):
    """recover_leases.sql calls its scalar subselect load-bearing and says not to
    simplify it away: inlined, the cutoff becomes a per-row filter and this reads
    every running task. The partial lease index is what keeps it a range seek."""
    got = plan(
        planned,
        "recover_leases",
        {"now_ms": 10**12, "lease_expired_error": "{}"},
    )
    assert "cairnq_tasks_lease_idx" in got, (
        f"recover_leases no longer reads the lease index — it now visits every "
        f"running task on every poll: {got}"
    )


def test_queue_depth_stays_bounded(planned):
    """queue_depth exists to be affordable to ask often: it must read at most
    :max_depth index entries, never count the whole backlog."""
    got = plan(planned, "queue_depth", {"queue": "default", "max_depth": 100})
    # Either claim index serves it — both lead with (queue, status), and which one
    # SQLite picks as the cheaper cover is not the property worth pinning. That
    # the probe is a SEARCH on that prefix rather than a scan of the table is.
    assert "cairnq_tasks_claim" in got, got
    assert "SCAN cairnq_tasks" not in got, f"queue_depth now scans the table: {got}"


def test_stats_filtered_to_one_queue_reads_only_that_queue(planned):
    """The reason stats takes a queue at all. Unfiltered it aggregates the table;
    filtered it should reach the (queue, status) prefix of the claim index."""
    got = plan(planned, "stats_one_queue", {"queue": "default"})
    assert "cairnq_tasks_claim" in got, got
    assert "SCAN cairnq_tasks" not in got, (
        f"stats(queue) reads the whole table, so narrowing it buys nothing: {got}"
    )
