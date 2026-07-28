"""Concurrent cold start. The migration path takes a write lock and re-checks
whether a migration is applied *inside* it, so two processes opening the same
fresh database can't both decide it is unapplied and both run it. The other
migration tests only cover the sequential cases, which would pass even without
the lock."""

from __future__ import annotations

import asyncio
import sqlite3

from cairnq import CairnQ
from cairnq._sql import load_migrations


async def test_concurrent_cold_start_applies_each_migration_once(db_path):
    # Separate clients means separate SQLite connections, so they contend on the
    # database file exactly as separate processes would.
    clients = [CairnQ.sqlite(db_path) for _ in range(4)]
    try:
        # Bounded so a regression that reintroduces contention fails the test
        # instead of hanging the suite.
        await asyncio.wait_for(asyncio.gather(*(c.connect() for c in clients)), timeout=30)
    finally:
        await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)

    conn = sqlite3.connect(db_path)
    try:
        rows = list(conn.execute("select name, count(*) from cairnq_migrations group by name"))
    finally:
        conn.close()

    # What each migration *does* is test_migrations.py's job; this one only cares
    # that concurrency didn't make any of them run twice.
    assert {name for name, _ in rows} == {name for name, _ in load_migrations("sqlite")}
    assert all(count == 1 for _, count in rows), f"a migration ran more than once: {rows}"


async def test_concurrent_cold_start_leaves_a_usable_store(db_path):
    clients = [CairnQ.sqlite(db_path) for _ in range(4)]
    await asyncio.gather(*(c.connect() for c in clients))
    try:
        tasks = await asyncio.gather(*(c.submit("job", {"i": i}) for i, c in enumerate(clients)))
        assert len({t.id for t in tasks}) == len(clients)
    finally:
        await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)
