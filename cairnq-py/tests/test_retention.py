"""Retention as a handle option rather than a chore.

`purge` is the only thing that removes rows, and a queue whose payloads carry real
data leaks disk until someone remembers to schedule it. These pin what the
built-in sweep does — and, as much, what it refuses to do: purge on startup, hold
the write lock for a whole backlog, or outlive the store it writes to.
"""

from __future__ import annotations

import asyncio

import pytest

from cairnq import CairnQ, Retention, RetentionSweeper


async def _finish_one(client: CairnQ) -> str:
    """Run a task to `succeeded`, so it is eligible for purge."""
    task = await client.submit("job", {})
    (claimed,) = await client.store.claim(queues=["default"], worker_id="w1", lease_ms=5_000)
    await client.store.succeed(task_id=claimed.id, worker_id="w1", result={})
    return task.id


async def _wait_for(cond, timeout_s: float = 3.0) -> None:
    """Wait until `cond` holds, or give up so the test's own assertion reports
    what went wrong instead of an opaque timeout."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if await cond():
            return
        await asyncio.sleep(0.01)


async def test_deletes_terminal_tasks_past_the_cutoff(db_path):
    async with CairnQ.sqlite(
        db_path, retention=Retention(older_than_ms=0, interval_ms=20)
    ) as client:
        done = await _finish_one(client)
        live = await client.submit("job", {})

        await _wait_for(lambda: _is_gone(client, done))
        assert await client.get(done) is None
        # Only terminal rows go: the queued task is still there to be claimed.
        assert (await client.get(live.id)).status == "queued"


async def _is_gone(client: CairnQ, task_id: str) -> bool:
    return await client.get(task_id) is None


async def test_starts_without_an_explicit_connect(db_path):
    # connect() is optional — every operation connects lazily through it — so
    # retention that only ran for callers who remembered to call it would be a
    # silent leak in the feature that exists to prevent one.
    client = CairnQ.sqlite(db_path, retention=Retention(older_than_ms=0, interval_ms=20))
    try:
        done = await _finish_one(client)  # first store touch: no connect() above
        await _wait_for(lambda: _is_gone(client, done))
        assert await client.get(done) is None
    finally:
        await client.close()


async def test_keeps_tasks_that_have_not_aged_out(db_path):
    async with CairnQ.sqlite(
        db_path, retention=Retention(older_than_ms=3_600_000, interval_ms=20)
    ) as client:
        done = await _finish_one(client)
        await asyncio.sleep(0.08)
        assert await client.get(done) is not None


async def test_does_not_purge_on_startup(db_path):
    # A process that restarts often would otherwise issue a write burst on every
    # boot — exactly when the store is busiest.
    async with CairnQ.sqlite(
        db_path, retention=Retention(older_than_ms=0, interval_ms=60_000)
    ) as client:
        done = await _finish_one(client)
        await asyncio.sleep(0.05)
        assert await client.get(done) is not None


async def test_drains_a_backlog_larger_than_one_statement(client):
    for _ in range(7):
        await _finish_one(client)

    # Directly, so the assertion is about one sweep rather than about timing.
    sweeper = RetentionSweeper(client.store, Retention(older_than_ms=0, limit=2))
    assert await sweeper.sweep() == 7
    assert await client.list() == []


async def test_reports_a_failed_sweep_and_keeps_sweeping(db_path):
    errors: list[BaseException] = []
    async with CairnQ.sqlite(
        db_path,
        retention=Retention(older_than_ms=0, interval_ms=20, on_error=errors.append),
    ) as client:
        store = client.store
        real_purge = store.purge
        failures = 2

        async def flaky(**kwargs):
            nonlocal failures
            if failures > 0:
                failures -= 1
                raise RuntimeError("database is locked")
            return await real_purge(**kwargs)

        store.purge = flaky
        done = await _finish_one(client)

        # A purge that failed because the database was busy is not a reason to
        # stop retaining, so the schedule survives it.
        await _wait_for(lambda: _is_gone(client, done))
        assert await client.get(done) is None
        assert len(errors) == 2
        store.purge = real_purge


async def test_stops_on_close_without_leaving_a_purge_behind(db_path):
    client = CairnQ.sqlite(db_path, retention=Retention(older_than_ms=0, interval_ms=10))
    await client.connect()
    done = await _finish_one(client)
    await _wait_for(lambda: _is_gone(client, done))
    # close() awaits the sweep in flight, so nothing can be mid-write against a
    # store that is already gone.
    await client.close()

    async with CairnQ.sqlite(db_path) as reopened:
        assert await reopened.get(done) is None


def test_refuses_a_cutoff_or_interval_that_cannot_mean_anything():
    with pytest.raises(ValueError, match="older_than_ms"):
        Retention(older_than_ms=-1)
    with pytest.raises(ValueError, match="interval_ms"):
        Retention(older_than_ms=0, interval_ms=0)
    with pytest.raises(ValueError, match="limit"):
        Retention(older_than_ms=0, limit=0)
