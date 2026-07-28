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
    real_claim = store.claim
    calls = 0

    async def flaky_claim(**kwargs):
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise sqlite3.OperationalError("database is locked")
        return await real_claim(**kwargs)

    store.claim = flaky_claim
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
    real_claim = store.claim
    calls = {"n": 0}

    async def broken_claim(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return await real_claim(**kwargs)
        return object()  # a store that breaks its own contract: truthy, not a list

    store.claim = broken_claim
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
