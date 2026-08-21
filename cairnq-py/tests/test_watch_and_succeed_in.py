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


class _Session:
    """Answers the connect-path statements; records everything else."""

    def __init__(self, calls: list, complete_matches: bool):
        self.calls = calls
        self._complete_matches = complete_matches

    async def query(self, text: str, values) -> list:
        if "current_schema()" in text:
            return [{"current_schema": None, "installations": []}]
        if "protocol_version" in text and "select" in text:
            return [{"value": "1"}]
        if "update cairnq_tasks" in text and "succeeded" in text:
            self.calls.append("complete")
            return [_row()] if self._complete_matches else []
        self.calls.append("query")
        return []

    async def execute(self, sql: str) -> None:
        self.calls.append("execute")


class _Executor:
    def __init__(self, complete_matches: bool = True, listen=None):
        self.calls: list = []
        self.rolled_back = False
        self._session = _Session(self.calls, complete_matches)
        if listen is not None:
            self.listen = listen

    async def query(self, text: str, values) -> list:
        return await self._session.query(text, values)

    async def execute(self, sql: str) -> None:
        await self._session.execute(sql)

    def transaction(self):
        executor = self

        class _Txn:
            async def __aenter__(self):
                executor.calls.append("BEGIN")
                return executor._session

            async def __aexit__(self, exc_type, exc, tb):
                if exc_type is None:
                    executor.calls.append("COMMIT")
                else:
                    executor.rolled_back = True
                    executor.calls.append("ROLLBACK")
                return False

        return _Txn()

    async def close(self) -> None:
        pass


def _context(store: PostgresStore) -> TaskContext:
    return TaskContext(store, Task.from_row(_row("running")), "w1", 30_000)


# ------------------------------------------------------------- succeed_in

async def test_commits_the_callers_writes_and_the_settlement_together():
    executor = _Executor()
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
    executor = _Executor(complete_matches=False)
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
    store = PostgresStore(_Executor())
    await store.connect()
    ctx = _context(store)
    assert await ctx.succeed_in(lambda _s: _none()) is not None
    assert await ctx.succeed_in(lambda _s: _none()) is None


async def _none():
    return None


async def test_a_sqlite_store_says_plainly_that_it_cannot_do_this(client):
    with pytest.raises(NotImplementedError, match="cannot share a transaction"):
        await client.store.complete_in(task_id="t1", worker_id="w1", write=lambda _s: _none())


# ------------------------------------------------------------------ watch

async def test_watch_delivers_queued_and_done_signals():
    captured = {}

    async def listen(channels, on_notify, on_close):
        captured["notify"] = on_notify
        return lambda: None

    store = PostgresStore(_Executor(listen=listen))
    await store.connect()
    seen: list[WatchSignal] = []
    # A poll interval far beyond the test, so anything observed came from push.
    stop = store.watch(seen.append, poll_ms=60_000)
    try:
        for _ in range(50):
            if "notify" in captured:
                break
            await asyncio.sleep(0.002)
        captured["notify"]("cairnq_queued", "render")
        captured["notify"]("cairnq_done", "task-7")
        assert seen == [
            WatchSignal(reason="queued", queue="render"),
            WatchSignal(reason="done", task_id="task-7"),
        ]
    finally:
        stop()


async def test_watch_drops_queues_it_was_not_asked_about():
    captured = {}

    async def listen(channels, on_notify, on_close):
        captured["notify"] = on_notify
        return lambda: None

    store = PostgresStore(_Executor(listen=listen))
    await store.connect()
    seen: list[WatchSignal] = []
    stop = store.watch(seen.append, queues=["render"], poll_ms=60_000)
    try:
        for _ in range(50):
            if "notify" in captured:
                break
            await asyncio.sleep(0.002)
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
    captured = {}

    async def listen(channels, on_notify, on_close):
        captured["notify"] = on_notify
        return lambda: None

    store = PostgresStore(_Executor(listen=listen))
    await store.connect()
    seen: list[WatchSignal] = []

    def boom(_signal):
        raise RuntimeError("consumer bug")

    stop_a = store.watch(boom, poll_ms=60_000)
    stop_b = store.watch(seen.append, poll_ms=60_000)
    try:
        for _ in range(50):
            if "notify" in captured:
                break
            await asyncio.sleep(0.002)
        captured["notify"]("cairnq_queued", "render")
        assert len(seen) == 1
    finally:
        stop_a()
        stop_b()
