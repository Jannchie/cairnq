"""Migration application. The interesting case is not a fresh database — every
other test covers that — but an existing one created by an older SDK, which must
pick up later migrations without re-running the ones it already has."""

from __future__ import annotations

import sqlite3

from cairnq import CairnQ
from cairnq._ids import now_ms
from cairnq._sql import load_migrations


#: Indexes added by migrations after 0001, so a database that only ran the first
#: one is missing all of them until it is upgraded.
_EXPECTED_INDEXES = {
    "cairnq_tasks_completed_idx",
    "cairnq_tasks_lease_idx",
    "cairnq_tasks_claim_name_idx",
    "cairnq_tasks_status_completed_idx",
}


def _index_names(db_path: str) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        return {r[0] for r in conn.execute("select name from sqlite_master where type='index'")}
    finally:
        conn.close()


def _meta(db_path: str, key: str) -> str | None:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("select value from cairnq_meta where key = ?", (key,)).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


async def test_applies_every_migration_to_a_fresh_database(db_path):
    client = CairnQ.sqlite(db_path)
    await client.connect()
    await client.close()

    conn = sqlite3.connect(db_path)
    try:
        applied = {r[0] for r in conn.execute("select name from cairnq_migrations")}
    finally:
        conn.close()

    assert applied == {name for name, _ in load_migrations("sqlite")}
    assert _EXPECTED_INDEXES <= _index_names(db_path)
    assert _meta(db_path, "schema_version") == "7"


async def test_upgrades_a_database_left_at_an_older_migration(db_path):
    """Simulate a database written by an SDK that only had 0001."""
    first_name, first_sql = load_migrations("sqlite")[0]
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(first_sql)
        conn.execute(
            "create table if not exists cairnq_migrations "
            "(name text primary key, applied_at_ms integer not null)"
        )
        conn.execute(
            "insert into cairnq_migrations (name, applied_at_ms) values (?, ?)",
            (first_name, now_ms()),
        )
        conn.commit()
    finally:
        conn.close()

    assert "cairnq_tasks_completed_idx" not in _index_names(db_path)
    assert _meta(db_path, "schema_version") == "1"

    client = CairnQ.sqlite(db_path)
    await client.connect()
    try:
        # The later migrations ran, and the store is usable afterwards.
        assert _EXPECTED_INDEXES <= _index_names(db_path)
        assert _meta(db_path, "schema_version") == "7"
        task = await client.submit("job", {"v": 1})
        assert (await client.get(task.id)).payload == {"v": 1}
    finally:
        await client.close()


async def test_reopening_does_not_reapply(db_path):
    for _ in range(3):
        client = CairnQ.sqlite(db_path)
        await client.connect()
        await client.close()

    conn = sqlite3.connect(db_path)
    try:
        rows = list(conn.execute("select name, count(*) from cairnq_migrations group by name"))
    finally:
        conn.close()
    assert all(count == 1 for _, count in rows)
