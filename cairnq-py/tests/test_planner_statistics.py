"""Keeping sqlite_stat1 current, at open and as a connection ages.

Without statistics SQLite misreads `status = 'running'` as a large fraction of the
table and passes over the partial cairnq_tasks_lease_idx that lease recovery is
indexed for — so migration 0004's index and the store's `PRAGMA optimize` are one
feature, not two. Nothing else fails when either half rots: lease recovery just
quietly goes back to scanning every running task."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from cairnq import CairnQ
from cairnq._sql import load_statements
from cairnq.store import sqlite as sqlite_store


def analyzed_rows(path: str, idx: str) -> int:
    """Rows sqlite_stat1 currently credits to an index — 0 when it has no entry."""
    db = sqlite3.connect(path)
    try:
        row = db.execute("select stat from sqlite_stat1 where idx = ?", (idx,)).fetchone()
        return int(row[0].split(" ")[0]) if row else 0
    finally:
        db.close()


def seed(path: str, n: int, status: str) -> None:
    """Write `n` tasks from another connection, as another process would."""
    db = sqlite3.connect(path)
    try:
        db.executemany(
            "insert into cairnq_tasks (id,name,queue,status,payload,run_at_ms,"
            "lease_until_ms,created_at_ms,updated_at_ms,completed_at_ms) "
            f"values (?,'job','default','{status}','{{}}',?,?,?,?,?)",
            [
                (
                    f"{status}_{i}",
                    i,
                    2**42 + i if status == "running" else None,
                    i,
                    i,
                    None if status == "running" else i,
                )
                for i in range(n)
            ],
        )
        db.commit()
    finally:
        db.close()


async def test_analyzes_on_connect_so_recover_leases_uses_its_partial_index(db_path):
    client = CairnQ.sqlite(db_path)
    await client.connect()
    await client.close()

    # The shape the index is designed for, and the reason it beats a plain scan of
    # 'running': a large drift of terminal rows (lease null — see PROTOCOL.md
    # §Lease model) over a handful of live leases bounded by worker concurrency.
    seed(db_path, 2_000, "succeeded")
    seed(db_path, 8, "running")

    reopened = CairnQ.sqlite(db_path)
    await reopened.connect()
    await reopened.close()

    assert analyzed_rows(db_path, "cairnq_tasks_lease_idx") > 0

    db = sqlite3.connect(db_path)
    try:
        plan = db.execute(
            f"explain query plan {load_statements('sqlite')['recover_leases']}",
            {"now_ms": 0, "lease_expired_error": "{}"},
        ).fetchall()
        assert "cairnq_tasks_lease_idx" in "\n".join(str(r[3]) for r in plan)
    finally:
        db.close()


async def test_re_analyzes_a_connection_that_outlived_its_statistics(
    db_path, monkeypatch: pytest.MonkeyPatch
):
    """The case the connect-time analyze cannot reach: a worker that started against
    an empty database and is still running once the backlog is real. Its statements
    were prepared against the empty shape, and it never reconnects.

    A second of real interval rather than a patched-to-zero one, so the throttle is
    exercised too: without it every operation would analyze, which is 15ms of write
    lock per call at a few hundred thousand rows."""
    monkeypatch.setattr(sqlite_store, "_OPTIMIZE_INTERVAL_S", 1.0)

    client = CairnQ.sqlite(db_path)
    await client.connect()
    try:
        seed(db_path, 5_000, "queued")
        idx = "cairnq_tasks_claim_idx"

        await client.get("absent")
        assert analyzed_rows(db_path, idx) < 100

        await asyncio.sleep(1.1)
        await client.get("absent")
        assert analyzed_rows(db_path, idx) >= 5_000
    finally:
        await client.close()
