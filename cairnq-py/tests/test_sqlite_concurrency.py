"""Several handles on one database, and the in-memory database, inside one process.

The TypeScript SDK holds SQLite's write lock across `await`s on a synchronous
driver, so two handles on one file there could deadlock the only thread. Python's
driver runs each connection on its own thread and has no such inversion — this
pins that, so the two SDKs are known to agree rather than assumed to.

The in-memory case is shared: WAL is a property of an on-disk file, and waiting
for an in-memory database to report `journal_mode = wal` waits for something that
can never happen."""

from __future__ import annotations

import asyncio

from cairnq import CairnQ


async def test_connects_to_an_in_memory_database():
    client = CairnQ.sqlite(":memory:")
    await client.connect()
    try:
        task = await client.submit("job", {"n": 1})
        current = await client.get(task.id)
        assert current is not None and current.queued
    finally:
        await client.close()


async def test_in_memory_handles_stay_independent():
    """Two :memory: databases share a path but not a database."""
    a, b = CairnQ.sqlite(":memory:"), CairnQ.sqlite(":memory:")
    await a.connect()
    await b.connect()
    try:
        await asyncio.gather(
            a.submit("job", {}, key="k"),
            b.submit("job", {}, key="k"),
        )
        assert await a.get_by_key("k") is not None
        assert await b.get_by_key("k") is not None
    finally:
        await a.close()
        await b.close()


async def test_two_handles_on_one_file_write_concurrently(db_path):
    a, b = CairnQ.sqlite(db_path), CairnQ.sqlite(db_path)
    await a.connect()
    await b.connect()
    try:
        # Keyed submits, so each goes through a transaction rather than a single
        # statement — that is what holds the write lock across an await.
        tasks = await asyncio.wait_for(
            asyncio.gather(
                *(a.submit("job", {"i": i}, key=f"a{i}") for i in range(10)),
                *(b.submit("job", {"i": i}, key=f"b{i}") for i in range(10)),
            ),
            timeout=30,
        )
        assert len({t.id for t in tasks}) == 20
    finally:
        await a.close()
        await b.close()
