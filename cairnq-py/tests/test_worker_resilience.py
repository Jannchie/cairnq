"""Worker resilience. Each test here pins a way the run loop used to give up or
stay silent rather than carry on:
  - a store error while finalizing a task vanished into an un-retrieved asyncio
    task, so the operator saw nothing at all;
  - a transient error from claim() propagated out of run() and ended the loop;
  - a lost lease was invisible to the handler, which kept doing side effects next
    to the task's new owner."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from cairnq import Worker
from cairnq.errors import LostLease
from cairnq.store.sqlite import SQLiteStore

from .helpers import wait_for


async def test_store_error_while_finalizing_is_reported_not_swallowed(client, db_path):
    store = SQLiteStore(db_path)
    await store.connect()

    async def boom(**_kwargs):
        raise RuntimeError("disk I/O error")

    store.complete = boom
    errors: list[BaseException] = []
    worker = Worker(
        store, ["default"], poll_interval_ms=20, on_error=lambda exc, info: errors.append(exc)
    )

    @worker.task("job")
    async def job(ctx):
        return {"ok": True}

    async with worker.background():
        await client.submit("job", {})
        await wait_for(lambda: len(errors) > 0)
    await store.close()

    assert errors, "a store failure while finalizing must reach on_error"
    assert "disk I/O error" in str(errors[0])


async def test_keeps_polling_after_a_transient_claim_error(client, db_path):
    store = SQLiteStore(db_path)
    await store.connect()
    real_claim = store.claim_session
    calls = 0

    async def flaky_claim(**kwargs):
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise sqlite3.OperationalError("database is locked")
        return await real_claim(**kwargs)

    store.claim_session =flaky_claim
    errors: list[BaseException] = []
    worker = Worker(
        store, ["default"], poll_interval_ms=10, on_error=lambda exc, info: errors.append(exc)
    )

    @worker.task("job")
    async def job(ctx):
        return {"ok": True}

    async with worker.background():
        result = await client.call("job", {}, wait_timeout_ms=5_000, poll_ms=20)
    await store.close()

    assert result == {"ok": True}
    assert len(errors) >= 2


async def test_lost_lease_is_signalled_to_the_running_handler(client, db_path):
    worker = Worker.sqlite(
        db_path,
        queues=["default"],
        poll_interval_ms=20,
        lease_ms=5_000,
        heartbeat_interval_ms=30,
    )
    observed = {"flag": False, "event": False}

    @worker.task("job")
    async def job(ctx):
        for _ in range(300):
            if ctx.lost_lease:
                break
            await asyncio.sleep(0.01)
        observed["flag"] = ctx.lost_lease
        observed["event"] = ctx.lease_lost.is_set()
        return {"ok": True}

    async with worker.background():
        t = await client.submit("job", {})

        def is_running() -> bool:
            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "select status from cairnq_tasks where id = ?", (t.id,)
                ).fetchone()
                return bool(row and row[0] == "running")
            finally:
                conn.close()

        await wait_for(is_running)
        # Simulate another worker taking the task over mid-flight.
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "update cairnq_tasks set worker_id = 'someone_else' where id = ?", (t.id,)
            )
            conn.commit()
        finally:
            conn.close()
        await wait_for(lambda: observed["flag"])

    assert observed["flag"] is True
    assert observed["event"] is True


async def test_run_drains_in_flight_tasks_however_it_exits(db_path, client):
    """run() promises that when it returns, nothing it started is still running —
    serve() closes the store the moment it returns, and a handler still holding
    the connection would fault. Pinned in both SDKs so the guarantee is known to
    hold on each rather than assumed from one."""
    store = SQLiteStore(db_path)
    await store.connect()
    real_claim = store.claim_session
    calls = {"n": 0}

    async def broken_claim(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return await real_claim(**kwargs)
        return object()  # a store that breaks its own contract: truthy, not a list

    store.claim_session =broken_claim
    finished = {"flag": False}
    # Two slots, so the loop comes back around to the broken claim while the first
    # task is still running.
    worker = Worker(store, ["default"], poll_interval_ms=5, concurrency=2)

    @worker.task("job")
    async def job(ctx):
        await asyncio.sleep(0.2)
        finished["flag"] = True
        return {}

    await client.submit("job", {})
    with pytest.raises(TypeError):
        await worker.run()
    assert finished["flag"], "run() returned while a handler was still running"
    await store.close()


# --------------------------------------------------------------- max_run_ms
# Why the ceiling exists: see the max_run_ms comment in Worker.__init__.


async def test_hung_handler_is_abandoned_at_max_run_ms(client, db_path):
    worker = Worker.sqlite(db_path, poll_interval_ms=20, max_run_ms=100)
    cancelled = asyncio.Event()

    @worker.task("hang")
    async def hang(ctx):
        try:
            await asyncio.Event().wait()  # never set: hung for good
        finally:
            cancelled.set()

    task = await client.submit("hang", {}, max_attempts=1)
    async with worker.background():
        final = await client.wait(task.id, timeout_ms=3_000, poll_ms=20)
        # The handler task is genuinely cancelled, not left running.
        await asyncio.wait_for(cancelled.wait(), 2)

    assert final.failed
    assert final.error["code"] == "handler_timeout"


async def test_timed_out_attempt_is_retried_with_backoff(client, db_path):
    worker = Worker.sqlite(db_path, poll_interval_ms=20, max_run_ms=100, retry_backoff_ms=1)
    attempts: list[int] = []

    @worker.task("flaky")
    async def flaky(ctx):
        attempts.append(ctx.attempt)
        if ctx.attempt == 1:
            await asyncio.Event().wait()  # hang only on the first attempt
        return {"attempt": ctx.attempt}

    task = await client.submit("flaky", {})
    async with worker.background():
        final = await client.wait(task.id, timeout_ms=3_000, poll_ms=20)

    assert final.succeeded
    assert final.result == {"attempt": 2}
    assert attempts == [1, 2]


async def test_timeout_frees_the_concurrency_slot(client, db_path):
    # concurrency=1: the hung task occupies the only slot until the ceiling
    # abandons it; the queued task must then run rather than wait for a lease.
    worker = Worker.sqlite(db_path, poll_interval_ms=20, max_run_ms=100, lease_ms=60_000)

    @worker.task("hang")
    async def hang(ctx):
        await asyncio.Event().wait()

    @worker.task("quick")
    async def quick(ctx):
        return {"ok": True}

    await client.submit("hang", {}, max_attempts=1)
    async with worker.background():
        result = await client.call("quick", {}, wait_timeout_ms=3_000, poll_ms=20)

    assert result == {"ok": True}


async def test_ctx_writes_short_circuit_once_the_lease_is_lost():
    """Why writes must short-circuit locally, not just at the store's
    ownership check: see TaskContext._owned."""
    from cairnq.context import TaskContext
    from cairnq.models import Task

    class ExplodingStore:
        def __getattr__(self, name):
            raise AssertionError("a lease-lost context must not touch the store")

    task = Task(id="t1", name="job", queue="default", status="running", payload={}, metadata={})
    ctx = TaskContext(ExplodingStore(), task, "w1", 1_000)
    ctx._mark_lease_lost()
    with pytest.raises(LostLease):
        await ctx.progress(0.5)
    with pytest.raises(LostLease):
        await ctx.heartbeat()
