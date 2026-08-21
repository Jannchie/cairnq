"""The two capabilities the executor seam exists for, on the Python side.

Mirrors `watch.test.ts` and the succeedIn half of `pg-executor.test.ts`: the
same contracts, tested the same way, because "everything above the storage seam
is identical" is a claim this SDK has to keep too.
"""

from __future__ import annotations

import asyncio

import pytest

from cairnq import CairnQ, LostLease, WatchSignal
from cairnq.context import TaskContext
from cairnq.models import Task
from .conftest import FakeExecutor

pytest.importorskip("asyncpg")

from cairnq.store.postgres import PostgresStore  # noqa: E402


def _row(status: str = "succeeded") -> dict:
    now = 1_700_000_000_000
    return {
        "id": "t1", "name": "render", "queue": "default", "status": status,
        "payload": {}, "metadata": {}, "result": None, "error": None,
        "progress": None, "message": None, "attempt": 1, "max_attempts": 3,
        "priority": 0, "worker_id": "w1", "lease_until_ms": None,
        "run_at_ms": now, "cancel_requested_at_ms": None, "parent_id": None,
        "root_id": None, "correlation_id": None, "created_at_ms": now,
        "updated_at_ms": now, "completed_at_ms": now,
    }


def _executor(**kwargs) -> FakeExecutor:
    return FakeExecutor(completed_row=_row(), **kwargs)


async def _pushing_store(): 
    """A connected store over an executor whose LISTEN is under test control.

    Returns the store and a `notify(channel, payload)` that fires what the
    database would have. The subscription is established synchronously by
    watch() -> _subscribe_push -> _listener_ready, so awaiting the task it
    creates is deterministic — no sleeping on it.
    """
    captured: dict = {}

    async def listen(_channels, on_notify, _on_close):
        captured["notify"] = on_notify
        return lambda: None

    store = PostgresStore(_executor(listen=listen))
    await store.connect()
    return store, captured


async def _settled(store: PostgresStore):
    """Await the in-flight LISTEN subscription, if watch() started one."""
    if store._listener_connecting is not None:
        await store._listener_connecting


def _context(store: PostgresStore) -> TaskContext:
    return TaskContext(store, Task.from_row(_row("running")), "w1", 30_000)


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


# ------------------------------------------------------------------ watch

async def test_watch_delivers_queued_and_done_signals():
    store, captured = await _pushing_store()
    seen: list[WatchSignal] = []
    # A poll interval far beyond the test, so anything observed came from push.
    stop = store.watch(seen.append, poll_ms=60_000)
    try:
        await _settled(store)
        captured["notify"]("cairnq_queued", "render")
        captured["notify"]("cairnq_done", "task-7")
        assert seen == [
            WatchSignal(reason="queued", queue="render"),
            WatchSignal(reason="done", task_id="task-7"),
        ]
    finally:
        stop()


async def test_watch_drops_queues_it_was_not_asked_about():
    store, captured = await _pushing_store()
    seen: list[WatchSignal] = []
    stop = store.watch(seen.append, queues=["render"], poll_ms=60_000)
    try:
        await _settled(store)
        captured["notify"]("cairnq_queued", "ingest")
        captured["notify"]("cairnq_queued", "render")
        # A done notification names only the task — which queue it was on is not
        # in the payload, so it is never filtered out.
        captured["notify"]("cairnq_done", "task-7")
        assert seen == [
            WatchSignal(reason="queued", queue="render"),
            WatchSignal(reason="done", task_id="task-7"),
        ]
    finally:
        stop()


async def test_watch_keeps_signalling_where_there_is_no_push_channel(client):
    # SQLite has no channel; the consumer must still be told to re-read.
    seen: list[WatchSignal] = []
    stop = client.watch(seen.append, poll_ms=10)
    try:
        await asyncio.sleep(0.05)
    finally:
        stop()
    assert len(seen) >= 2
    assert all(s.reason == "poll" for s in seen)

    before = len(seen)
    await asyncio.sleep(0.05)
    # A signal after unsubscribe would have the consumer re-reading a store it
    # has stopped caring about, possibly a closed one.
    assert len(seen) == before


async def test_one_subscribers_exception_does_not_cost_the_others_their_signal():
    store, captured = await _pushing_store()
    seen: list[WatchSignal] = []

    def boom(_signal):
        raise RuntimeError("consumer bug")

    stop_a = store.watch(boom, poll_ms=60_000)
    stop_b = store.watch(seen.append, poll_ms=60_000)
    try:
        await _settled(store)
        captured["notify"]("cairnq_queued", "render")
        assert len(seen) == 1
    finally:
        stop_a()
        stop_b()
