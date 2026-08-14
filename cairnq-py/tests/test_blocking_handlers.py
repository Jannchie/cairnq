"""Handlers that block, and the lease renewal that used to die with them.

The heartbeat runs on the worker's event loop. A handler that occupies that loop
for seconds — a GPU forward, a synchronous HTTP client, a hash over a large file
— stops the renewal too: the lease expires, another worker recovers the task, and
both compute the same thing. Nothing in the handler can see it happen.

So a sync handler is dispatched to a thread, and an async handler that blocks
anyway is reported rather than left to fail silently.
"""

from __future__ import annotations

import asyncio
import time

from cairnq import EventLoopBlocked, Worker

from .helpers import wait_for


async def test_a_sync_handler_does_not_starve_the_heartbeat(client, db_path):
    # The lease is short enough that a blocked loop loses it: at lease/3 the
    # handler below would miss four beats.
    worker = Worker.sqlite(db_path, poll_interval_ms=20, lease_ms=200)

    @worker.task("slow")
    def slow(ctx, payload):
        time.sleep(0.5)  # a sync handler, the way real blocking work arrives
        return {"ok": True}

    async with worker.background():
        task = await client.submit("slow", {})
        done = await client.wait(task.id, timeout_ms=5_000)

    assert done.status == "succeeded"
    assert done.result == {"ok": True}
    # The point: one attempt. A starved heartbeat would have let the lease expire
    # and handed the task to a second attempt while the first still ran.
    assert done.attempt == 1


async def test_the_loop_stays_live_while_a_sync_handler_runs(client, db_path):
    worker = Worker.sqlite(db_path, poll_interval_ms=20, lease_ms=5_000)
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    @worker.task("slow")
    def slow(ctx, payload):
        time.sleep(0.3)
        return {}

    beat = asyncio.create_task(ticker())
    try:
        async with worker.background():
            task = await client.submit("slow", {})
            await client.wait(task.id, timeout_ms=5_000)
    finally:
        beat.cancel()

    # Anything on the loop kept running: submits, other handlers, the heartbeat.
    assert ticks > 10, f"the loop stalled during the sync handler ({ticks} ticks)"


async def test_a_blocking_async_handler_is_reported(client, db_path):
    # Dispatching sync handlers to a thread cannot save an async one that blocks
    # inside itself, so that case is named instead of left to expire quietly.
    errors: list[BaseException] = []
    worker = Worker.sqlite(
        db_path,
        poll_interval_ms=20,
        lease_ms=300,
        on_error=lambda exc, info: errors.append(exc),
    )

    @worker.task("blocking")
    async def blocking(ctx, payload):
        time.sleep(0.6)  # blocking work inside an async handler: the real bug
        return {}

    async with worker.background():
        await client.submit("blocking", {})
        await wait_for(lambda: any(isinstance(e, EventLoopBlocked) for e in errors))

    blocked = [e for e in errors if isinstance(e, EventLoopBlocked)]
    assert blocked, "a blocked event loop must reach on_error"
    assert blocked[0].late_ms > blocked[0].interval_ms
    assert "asyncio.to_thread" in str(blocked[0])


async def test_a_healthy_worker_reports_nothing(client, db_path):
    # A production-shaped lease, so this asserts the detector's threshold rather
    # than the scheduling luck of the machine running it: a 250ms handler would
    # have to stall the loop for twenty seconds to be reported. Squeezing the
    # lease down to the handler's own duration makes ordinary jitter on a loaded
    # host — which is a real beat miss, and correctly reported — look like a bug.
    errors: list[BaseException] = []
    worker = Worker.sqlite(
        db_path,
        poll_interval_ms=20,
        lease_ms=30_000,
        on_error=lambda exc, info: errors.append(exc),
    )

    @worker.task("quick")
    async def quick(ctx, payload):
        await asyncio.sleep(0.25)
        return {}

    async with worker.background():
        task = await client.submit("quick", {})
        await client.wait(task.id, timeout_ms=5_000)

    assert not [e for e in errors if isinstance(e, EventLoopBlocked)]


async def test_a_callable_object_is_recognised_as_async(client, db_path):
    """`iscoroutinefunction` says False for an instance whose __call__ is async;
    reading it off the type is what keeps such a handler from being sent to a
    thread and awaited there."""

    class Handler:
        async def __call__(self, ctx, payload):
            await asyncio.sleep(0)
            return {"kind": "async-callable"}

    worker = Worker.sqlite(db_path, poll_interval_ms=20)
    worker.task("obj")(Handler())

    async with worker.background():
        task = await client.submit("obj", {})
        done = await client.wait(task.id, timeout_ms=5_000)

    assert done.result == {"kind": "async-callable"}
