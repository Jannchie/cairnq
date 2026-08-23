"""Handlers that block, and the lease renewal that used to die with them.

The heartbeat runs on the worker's event loop. A handler that occupies that loop
for seconds — a GPU forward, a synchronous HTTP client, a hash over a large file
— stops the renewal too: the lease expires, another worker recovers the task, and
both compute the same thing. Nothing in the handler can see it happen.

So a sync handler is dispatched to a thread, which is what keeps its lease alive.
"""

from __future__ import annotations

import asyncio
import time

from cairnq import Worker


async def test_a_sync_handler_does_not_starve_the_heartbeat(client, db_path):
    # The handler outlasts its lease, so running it on the loop loses that lease
    # — which is the regression. The margin between the two is deliberately wide:
    # squeezed to a 200ms lease this asserted that a shared CI runner never drops
    # three consecutive 66ms beats, which is not what it is here to prove.
    worker = Worker.sqlite(db_path, poll_interval_ms=20, lease_ms=600)

    @worker.task("slow")
    def slow(ctx, payload):
        time.sleep(1.5)  # a sync handler, the way real blocking work arrives
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
