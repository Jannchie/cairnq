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
import sqlite3

import pytest

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


# A lost write lock is waited out by an awaited backoff, not inside the connection
# thread. That thread is this store's only path to the database, so a write parked
# in busy_timeout holds it against every other operation in the process — the
# reads, the worker's poll, another task's heartbeat — on a lock that, under WAL,
# a reader never needed in the first place.
#
# The blocker is a plain connection, not a store: nothing serializes it against
# the store, which is what a competing *process* looks like from here. Twin of
# "waits out a contended write lock without blocking the event loop" in
# cairnq-node/test/sqlite-concurrency.test.ts.
@pytest.mark.parametrize("keyed", [False, True], ids=["single statement", "transaction"])
async def test_waits_out_a_contended_write_lock_without_stalling_reads(db_path, keyed):
    client = CairnQ.sqlite(db_path)
    await client.connect()

    blocker = sqlite3.connect(db_path, isolation_level=None)
    blocker.execute("pragma busy_timeout = 0")
    blocker.execute("BEGIN IMMEDIATE")

    async def release() -> None:
        await asyncio.sleep(0.3)
        blocker.execute("COMMIT")
        blocker.close()

    released = asyncio.create_task(release())
    reads = 0

    async def read_while_blocked() -> None:
        nonlocal reads
        while True:
            await client.get("absent")
            reads += 1

    reader = asyncio.create_task(read_while_blocked())
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        task = await client.submit("job", {"v": 1}, key="k" if keyed else None)
        elapsed = loop.time() - started

        # It really did have to wait for the blocker...
        assert elapsed > 0.25
        # ...and the store kept answering reads throughout, which is the half a
        # blocking busy_timeout takes away: they would have queued behind the
        # write on the connection thread and none would have landed.
        assert reads > 5
        assert (await client.get(task.id)).payload == {"v": 1}
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        await released
        await client.close()
