"""Group commit: writes that are already waiting on the store's lock share one
transaction.

A protocol write costs microseconds to execute and a WAL commit to durably land, so
concurrent writers spend nearly all their time on commits they could have shared.
Nothing waits to form a batch — a flusher takes whatever arrived while the previous
one held the lock — so the trade is not latency but atomicity: two callers' writes
now land together or not at all, which under at-least-once is a redelivery rather
than a lost task.

What each test here is really guarding is a way to get this wrong quietly: results
crossing between callers, one bad write poisoning its neighbours, a write arriving
mid-batch and being stranded, and reads being dragged into the write transaction.

Mirrors cairnq-node/test/group-commit.test.ts.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3

import pytest

from cairnq import CairnQ, LostLease, SQLiteStore, StoreClosed

WORKER = "w1"


def wal_frames(path: str, page_size: int) -> int:
    """Frames written to the WAL so far.

    The WAL is a 32-byte header plus fixed-size frames, and every commit appends at
    least one — so this counts commits from the outside, without the store having to
    report on itself. Repeated writes to one small table touch one page, which is
    what makes the count track transactions rather than data volume."""
    size = os.path.getsize(f"{path}-wal")
    return 0 if size <= 32 else (size - 32) // (24 + page_size)


def page_size_of(path: str) -> int:
    db = sqlite3.connect(path)
    try:
        return int(db.execute("pragma page_size").fetchone()[0])
    finally:
        db.close()


async def claimed(client: CairnQ, store: SQLiteStore, n: int) -> list[str]:
    """`n` claimed tasks, ready to be finalized."""
    await asyncio.gather(*(client.submit("job", {"i": i}) for i in range(n)))
    tasks = await store.claim(
        queues=["default"], worker_id=WORKER, lease_ms=600_000, limit=n, names=["job"]
    )
    assert len(tasks) == n
    return [t.id for t in tasks]


async def test_commits_concurrent_writes_together_and_sequential_writes_separately(db_path):
    client = CairnQ.sqlite(db_path)
    store = SQLiteStore(db_path)
    await client.connect()
    await store.connect()
    try:
        page_size = page_size_of(db_path)
        n = 32

        serial_ids = await claimed(client, store, n)
        before_serial = wal_frames(db_path, page_size)
        for task_id in serial_ids:
            await store.complete(task_id=task_id, worker_id=WORKER, result={})
        serial_frames = wal_frames(db_path, page_size) - before_serial

        batch_ids = await claimed(client, store, n)
        before_batch = wal_frames(db_path, page_size)
        await asyncio.gather(
            *(
                store.complete(task_id=task_id, worker_id=WORKER, result={})
                for task_id in batch_ids
            )
        )
        batch_frames = wal_frames(db_path, page_size) - before_batch

        # Awaited one at a time, nothing is ever waiting, so each write is its own
        # transaction — the uncontended path has to stay that cheap.
        assert serial_frames >= n
        # Issued together, they share one. Not asserted as exactly one commit: the
        # first writer takes the lock before the rest have been issued, so a real run
        # is a small handful of batches rather than a single one.
        assert batch_frames < serial_frames / 4
    finally:
        await client.close()
        await store.close()


async def test_gives_each_concurrent_writer_its_own_rows(db_path):
    client = CairnQ.sqlite(db_path)
    store = SQLiteStore(db_path)
    await client.connect()
    await store.connect()
    try:
        ids = await claimed(client, store, 16)
        # A distinct result per task: a batch that merged or misordered its rows would
        # hand somebody else's back.
        done = await asyncio.gather(
            *(
                store.complete(task_id=task_id, worker_id=WORKER, result={"i": i})
                for i, task_id in enumerate(ids)
            )
        )
        assert [t.id for t in done] == ids
        assert [t.result for t in done] == [{"i": i} for i in range(len(ids))]
    finally:
        await client.close()
        await store.close()


async def test_fails_only_the_writer_whose_own_write_failed(db_path):
    client = CairnQ.sqlite(db_path)
    store = SQLiteStore(db_path)
    await client.connect()
    await store.connect()
    try:
        ids = await claimed(client, store, 8)
        # One finalize by a worker that does not own the lease: its statement matches
        # no row, which is a lost lease. Sharing a transaction must not spread that to
        # the seven around it, nor roll their writes back.
        calls = [
            store.complete(
                task_id=task_id,
                worker_id="someone-else" if i == 4 else WORKER,
                result={},
            )
            for i, task_id in enumerate(ids)
        ]
        outcomes = await asyncio.gather(*calls, return_exceptions=True)
        failed = [i for i, o in enumerate(outcomes) if isinstance(o, BaseException)]
        assert failed == [4]
        assert isinstance(outcomes[4], LostLease)

        rows = await asyncio.gather(*(client.get(task_id) for task_id in ids))
        assert [t.status for t in rows] == [
            "succeeded",
            "succeeded",
            "succeeded",
            "succeeded",
            "running",
            "succeeded",
            "succeeded",
            "succeeded",
        ]
    finally:
        await client.close()
        await store.close()


async def test_picks_up_a_write_issued_while_a_batch_is_in_flight(db_path):
    client = CairnQ.sqlite(db_path)
    store = SQLiteStore(db_path)
    await client.connect()
    await store.connect()
    try:
        ids = await claimed(client, store, 12)
        first, chased = ids[:6], ids[6:]

        async def finalize_then_chase(i: int) -> None:
            # The window a naive flusher strands a write in: issued right after an
            # earlier one resolved, so it arrives after the batch was taken but
            # before the flusher has finished with it. If nothing re-checks, this
            # never resolves and the test hangs rather than failing an assertion.
            await store.complete(task_id=first[i], worker_id=WORKER, result={})
            await store.complete(task_id=chased[i], worker_id=WORKER, result={})

        await asyncio.wait_for(
            asyncio.gather(*(finalize_then_chase(i) for i in range(len(first)))), timeout=30
        )
        rows = await asyncio.gather(*(client.get(task_id) for task_id in ids))
        assert all(t.status == "succeeded" for t in rows)
    finally:
        await client.close()
        await store.close()


async def test_keeps_reads_out_of_the_write_transaction(db_path):
    client = CairnQ.sqlite(db_path)
    await client.connect()
    # Hold the write lock from a connection nothing serializes against — a competing
    # process, as far as the store is concerned.
    blocker = sqlite3.connect(db_path, isolation_level=None)
    blocker.execute("pragma busy_timeout = 0")
    blocker.execute("BEGIN IMMEDIATE")
    try:
        # Reads must still go through. If a read were batched with the writes it would
        # need the write lock this blocker is holding, and an idle worker's poll would
        # stall behind unrelated writers — the whole point of the read-only claim
        # probe. Bounded: on a regression this blocks for the busy timeout rather than
        # failing, and a hung test is a worse signal than a failed one.
        assert await asyncio.wait_for(client.get("absent"), timeout=10) is None
        assert await asyncio.wait_for(client.list(limit=1), timeout=10) == []
        assert await asyncio.wait_for(client.queue_depth("default", 10), timeout=10) >= 0
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()
        await client.close()


@pytest.mark.parametrize("n", [1, 5])
async def test_uncontended_writes_stay_one_transaction_each(db_path, n: int):
    """The batch-of-one path: a store nobody is competing with must not pay for
    BEGIN/COMMIT around every single write."""
    client = CairnQ.sqlite(db_path)
    store = SQLiteStore(db_path)
    await client.connect()
    await store.connect()
    try:
        ids = await claimed(client, store, n)
        for task_id in ids:
            task = await store.complete(task_id=task_id, worker_id=WORKER, result={})
            assert task.status == "succeeded"
            assert task.lease_until_ms is None
    finally:
        await client.close()
        await store.close()


async def test_lands_the_writes_already_queued_before_closing(db_path):
    """close() must not drop the connection while a group commit still holds
    writes their callers are awaiting. This SDK already waited for the flusher;
    the TypeScript twin did not, and lost the write to a TypeError about a
    connection that had gone away. Both wait now, and both also wait out an open
    transaction by taking the store lock."""
    client = CairnQ.sqlite(db_path)
    await client.connect()
    # Scheduled without awaiting, so they are still pending when close lands.
    in_flight = [asyncio.create_task(client.submit("a", {"i": i})) for i in range(3)]
    await asyncio.sleep(0)
    await client.close()
    landed = await asyncio.gather(*in_flight)
    assert len(landed) == 3

    # Durable, not merely resolved: a fresh handle sees all three.
    reader = CairnQ.sqlite(db_path)
    try:
        rows = await reader.list(name="a")
        assert sorted(r.payload["i"] for r in rows) == [0, 1, 2]
    finally:
        await reader.close()


async def test_turns_away_work_that_arrives_while_closing(db_path):
    """The drain has to be finite: without a barrier a write landing between the
    flusher finishing and the connection closing starts a fresh flusher against a
    connection about to go away. Anything arriving now gets a typed StoreClosed.

    Driven through the store rather than CairnQ, whose close() awaits its
    retention sweeper first — that yields a turn, long enough for a submit issued
    "during" close to actually arrive before it and be drained by the case
    above. The barrier is the store's, so it is tested there."""
    store = SQLiteStore(db_path)
    await store.connect()
    closing = asyncio.create_task(store.close())
    await asyncio.sleep(0)
    with pytest.raises(StoreClosed):
        await store.submit(name="a", payload={})
    await closing
    # Closing does not retire the store: a file-backed one reopens on next use.
    assert (await store.submit(name="a", payload={})).id
    await store.close()


async def test_refuses_to_reopen_an_in_memory_database_it_closed():
    """A file store reopens onto its tasks; an in-memory one would reopen onto
    nothing, because its contents lived in the connection. That used to succeed
    silently — every task gone, get() answering None as if the id had never
    existed."""
    mem = CairnQ.sqlite(":memory:")
    task = await mem.submit("a", {})
    assert await mem.get(task.id) is not None
    await mem.close()
    with pytest.raises(StoreClosed):
        await mem.submit("b", {})
    with pytest.raises(StoreClosed, match="silently start"):
        await mem.get(task.id)
