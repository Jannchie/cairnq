"""The capabilities the executor seam exists for, on the Python side.

Mirrors the succeedIn half of `pg-executor.test.ts`, plus the LISTEN wake
machinery that shares its listener: the same contracts, tested the same way,
because "everything above the storage seam is identical" is a claim this SDK has
to keep too.
"""

from __future__ import annotations

import asyncio

import pytest

from cairnq import LostLease
from cairnq.context import TaskContext
from cairnq.models import Task
from .conftest import FakeExecutor, task_row

pytest.importorskip("asyncpg")

from cairnq.store.postgres import PostgresStore  # noqa: E402


def _row(status: str = "succeeded") -> dict:
    # json_is_text=False: this fake's driver delivers decoded JSON columns, which
    # is also what its answer to the store's wire-form probe says (see conftest).
    return task_row(json_is_text=False, status=status)


def _executor(**kwargs) -> FakeExecutor:
    return FakeExecutor(completed_row=_row(), **kwargs)


async def _pushing_store(): 
    """A connected store over an executor whose LISTEN is under test control.

    Returns the store and a `notify(channel, payload)` that fires what the
    database would have. The subscription is started by the first wake call, so
    awaiting the task it creates (see _settled) is deterministic — no sleeping
    on it.
    """
    captured: dict = {}

    async def listen(_channels, on_notify, _on_close):
        captured["notify"] = on_notify
        return lambda: None

    store = PostgresStore(_executor(listen=listen))
    await store.connect()
    return store, captured


async def _settled(store: PostgresStore):
    """Await the in-flight LISTEN subscription, if a wake call started one."""
    if store._listener_connecting is not None:
        await store._listener_connecting


def _context(store: PostgresStore) -> TaskContext:
    # json_is_text=False: _row() is already decoded, as this fake's driver would
    # deliver it — the same answer the store gets from its wire-form probe.
    return TaskContext(store, Task.from_row(_row("running"), json_is_text=False), "w1", 30_000)


# ------------------------------------------------------------- succeed_in

async def test_commits_the_callers_writes_and_the_settlement_together():
    executor = _executor()
    store = PostgresStore(executor)
    await store.connect()
    executor.calls.clear()

    async def write(session):
        await session.query("insert into visual_pages (id) values ($1)", ["p1"])
        return {"pages": 1}

    task = await _context(store).succeed_in(write)
    assert task is not None and task.status == "succeeded"
    # One transaction; the caller's write inside it; the settlement last, so a
    # lease lost at the end takes the caller's write down with it.
    assert executor.calls[0] == "BEGIN"
    assert executor.calls[-1] == "COMMIT"
    assert executor.calls.index("query") < executor.calls.index("complete")


async def test_rolls_the_callers_writes_back_when_the_lease_was_gone():
    executor = _executor(complete_matches=False)
    store = PostgresStore(executor)
    await store.connect()
    executor.calls.clear()

    async def write(session):
        await session.query("insert into visual_pages (id) values ($1)", ["p1"])
        return None

    with pytest.raises(LostLease):
        await _context(store).succeed_in(write)
    # The whole point: no ordering exists in which those pages are durable and
    # the task still reads as running.
    assert executor.rolled_back is True
    assert "COMMIT" not in executor.calls


async def test_does_not_settle_twice():
    store = PostgresStore(_executor())
    await store.connect()
    ctx = _context(store)
    assert await ctx.succeed_in(lambda _s: _none()) is not None
    assert await ctx.succeed_in(lambda _s: _none()) is None


async def _none():
    return None


async def test_a_sqlite_store_says_plainly_that_it_cannot_do_this(client):
    from cairnq import UnsupportedBackend

    with pytest.raises(UnsupportedBackend, match="cannot share a transaction"):
        await client.store.complete_in(task_id="t1", worker_id="w1", write=lambda _s: _none())


# ------------------------------------------------------------ push wakeups


# close() does not take the init lock — it only drops the handle to whatever
# connect() is doing. That connect resumes afterwards, and if it publishes what it
# built, a store nobody will close again ends up owning a LISTEN connection.
async def test_does_not_install_a_listener_for_a_store_closed_while_connecting():
    listens = 0
    gate = asyncio.Event()

    async def listen(_channels, _on_notify, _on_close):
        nonlocal listens
        listens += 1
        return lambda: None

    executor = _executor(listen=listen)

    async def slow_execute(_sql: str) -> None:
        # Stands in for the migration round-trips: connect is mid-flight here.
        await gate.wait()

    executor.execute = slow_execute
    store = PostgresStore(executor)

    connecting = asyncio.create_task(store.connect())
    await asyncio.sleep(0)  # let connect reach the gate
    await store.close()
    gate.set()
    with pytest.raises(RuntimeError, match="closed while connecting"):
        await connecting

    # Nothing subscribed, and nothing can start one later either.
    assert listens == 0
    await store.claim_wake(["render"], 1)
    await _settled(store)
    assert listens == 0


# claim_wake's buffer. A notification that
# lands between two polls has to survive until the next claim_wake asks — but only
# for a queue somebody actually waits on, or the buffer is a leak that grows with
# every distinct queue name the database ever sees.
async def test_buffers_a_wake_for_a_waited_queue_and_only_for_those():
    store, captured = await _pushing_store()
    # Establishes both the subscription and what this process waits on.
    await store.claim_wake(["render"], 1)
    await _settled(store)

    captured["notify"]("cairnq_queued", "ingest")
    captured["notify"]("cairnq_queued", "user:1234")
    # Nothing waits on those, so nothing is remembered about them.
    assert store._pending_queues == set()

    captured["notify"]("cairnq_queued", "render")
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    # Buffered while nobody was waiting, so this returns on the notification
    # rather than waiting out its timeout.
    await store.claim_wake(["render"], 60_000)
    assert loop.time() - started_at < 1.0

    await store.close()
