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

Not every plan lives here. `claim`'s belongs to test_claim_due_order.py, which
pins it next to the ordering behaviour 0008 changed, and `recover_leases`' to
test_planner_statistics.py, which pins it next to the ANALYZE that makes the
planner choose it — in both cases the plan is one assertion in a larger argument
and moving it here would separate it from its reason. What is here is what had
no home.

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
from cairnq.store.base import specialize

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
        "completed_at_ms,root_id,correlation_id) values (?,?,?,?,'{}',0,?,null,?,0,3,?,?,?,?,?)",
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
                # Spread over enough distinct values that ANALYZE reports the
                # index as selective. All-NULL columns make it useless and the
                # planner correctly ignores it — which would test the fixture,
                # not the statement.
                f"r{i % 50}",
                f"c{i % 50}",
            )
            for i in range(ROWS)
        ],
    )
    conn.commit()
    conn.execute("analyze")
    yield conn
    conn.close()


def plan(conn: sqlite3.Connection, statement: str, params: dict) -> str:
    """The planner's own description of how it will run this statement — for the
    text the store would actually submit, which is `specialize`'s output and not
    the file's. Asking about the file would test something no caller ever runs."""
    sql = specialize(STATEMENTS[statement], params)
    return " | ".join(r[3] for r in conn.execute("explain query plan " + sql, params))


PURGE_BASE = {"before_ms": 10**12, "queue": None, "status": None, "name": None, "limit": 100}
LIST_BASE = {
    "status": None, "queue": None, "name": None,
    "root_id": None, "correlation_id": None, "limit": 50, "offset": 0,
}

# Every filtered shape these two statements can be asked for, and the index it
# must reach. Unfiltered they are meant to scan; filtered, the whole point is to
# read only the matching range instead of the table.
FILTERED_CASES = [
    ("purge", {"status": "succeeded"}, "cairnq_tasks_status_completed_idx", PURGE_BASE),
    ("purge", {"queue": "rpc"}, "cairnq_tasks_queue_completed_idx", PURGE_BASE),
    ("purge", {"queue": "rpc", "status": "succeeded"},
     "cairnq_tasks_queue_completed_idx", PURGE_BASE),
    # list's filters are the oldest instance of the same defect: migration 0001
    # shipped an index for each and none was read until specialize existed.
    ("list", {"root_id": "r7"}, "cairnq_tasks_root_idx", LIST_BASE),
    ("list", {"correlation_id": "c7"}, "cairnq_tasks_correlation_idx", LIST_BASE),
    ("list", {"name": "job1"}, "cairnq_tasks_name_idx", LIST_BASE),
    ("list", {"queue": "rpc"}, "cairnq_tasks_claim", LIST_BASE),
]


@pytest.mark.parametrize(
    "statement,filters,index,base",
    FILTERED_CASES,
    ids=[f"{c[0]}:{'+'.join(c[1])}" for c in FILTERED_CASES],
)
def test_a_filtered_statement_reaches_its_index(planned, statement, filters, index, base):
    got = plan(planned, statement, {**base, **filters})
    assert index in got, f"{statement} {filters} no longer reads {index}: {got}"
    assert "SCAN cairnq_tasks" not in got, (
        f"{statement} {filters} reads the whole table, so the filter buys nothing: {got}"
    )


def test_purge_unfiltered_reads_in_completion_order(planned):
    """The bounded-sweep property: `limit` can only stop early if the ORDER BY is
    served by the index rather than by a sort over everything past the cutoff."""
    got = plan(planned, "purge", PURGE_BASE)
    assert "cairnq_tasks_completed_idx" in got, got
    assert "TEMP B-TREE" not in got, f"purge now sorts instead of reading in order: {got}"


def test_queue_depth_stays_bounded(planned):
    """queue_depth exists to be affordable to ask often: it must read at most
    :max_depth index entries, never count the whole backlog."""
    got = plan(planned, "queue_depth", {"queue": "default", "max_depth": 100})
    # Either claim index serves it — both lead with (queue, status), and which one
    # SQLite picks as the cheaper cover is not the property worth pinning. That
    # the probe is a SEARCH on that prefix rather than a scan of the table is.
    assert "cairnq_tasks_claim" in got, got
    assert "SCAN cairnq_tasks" not in got, f"queue_depth now scans the table: {got}"
