"""Backpressure: the depth probe's semantics, the gate built on it, and the
worker-side byte budget.

Without these a producer that outruns its workers is bounded only by disk, and a
worker sized by task count holds concurrency * largest-payload bytes the moment
big payloads arrive. Each test here pins one half of that. The TypeScript SDK
carries the same set (test/backpressure.test.ts)."""

from __future__ import annotations

import asyncio

import pytest

from cairnq import CairnQ, QueueDepthGate, QueueFull, Worker
from cairnq.store.sqlite import SQLiteStore

from .helpers import wait_for


# ------------------------------------------------------------------ queue_depth
# The statement's own semantics — headroom, saturation at 0, queued-only,
# per-queue isolation, delayed tasks counting — live in the conformance scenario
# (cairnq-protocol/conformance/scenarios/queue_depth.json), which runs in both
# SDKs against both dialects. Repeating them here would be strictly weaker
# coverage in a second place to maintain. What stays is SDK-side validation,
# which the SQL never sees.
async def test_depth_rejects_a_negative_limit(client):
    with pytest.raises(ValueError, match=">= 0"):
        await client.queue_depth("default", -1)


# ------------------------------------------------------------- QueueDepthGate
async def test_lets_submits_through_under_the_limit(client, db_path):
    gated = CairnQ.sqlite(db_path, max_queue_depth=3)
    await gated.connect()
    try:
        for i in range(3):
            await gated.submit("job", {"i": i})
        assert await client.queue_depth("default", 10) == 7
    finally:
        await gated.close()


async def test_blocks_at_the_limit_and_proceeds_once_drained(client, db_path):
    gated = CairnQ.sqlite(db_path, max_queue_depth=2, queue_poll_interval_ms=20)
    await gated.connect()
    worker = Worker(SQLiteStore(db_path), ["default"], poll_interval_ms=20)
    try:
        await gated.submit("job", {"i": 0})
        await gated.submit("job", {"i": 1})

        blocked = asyncio.ensure_future(gated.submit("job", {"i": 2}))
        await asyncio.sleep(0.15)
        assert not blocked.done()  # still waiting on a full queue

        worker.register("job", lambda ctx, payload: {"ok": True})
        runner = asyncio.ensure_future(worker.run())
        # Not that it resolved — awaiting it already says that — but that the
        # task the gate held back actually reached the queue once room appeared.
        task = await blocked
        assert task.queue == "default"
        assert await client.get(task.id) is not None

        worker.stop()
        await runner
    finally:
        await worker.close()
        await gated.close()


async def test_raises_queue_full_on_timeout_and_enqueues_nothing(client, db_path):
    gated = CairnQ.sqlite(
        db_path, max_queue_depth=1, max_queue_wait_ms=120, queue_poll_interval_ms=20
    )
    await gated.connect()
    try:
        await gated.submit("job", {"i": 0})
        with pytest.raises(QueueFull):
            await gated.submit("job", {"i": 1})
        # The whole point of raising: the backlog did not grow past the limit.
        assert await client.queue_depth("default", 10) == 9
    finally:
        await gated.close()


async def test_gates_only_the_queues_a_per_queue_limit_names(client, db_path):
    gated = CairnQ.sqlite(
        db_path,
        max_queue_depth={"tight": 1},
        max_queue_wait_ms=60,
        queue_poll_interval_ms=20,
    )
    await gated.connect()
    try:
        await gated.submit("job", {}, queue="tight")
        with pytest.raises(QueueFull):
            await gated.submit("job", {}, queue="tight")
        # "loose" is not listed, so it is not gated at all.
        for i in range(5):
            await gated.submit("job", {"i": i}, queue="loose")
        assert await client.queue_depth("loose", 10) == 5
    finally:
        await gated.close()


async def test_amortizes_the_probe_across_a_grant(db_path):
    store = SQLiteStore(db_path)
    await store.connect()
    probes = 0
    real_depth = store.queue_depth

    async def counting(queue: str, max_depth: int) -> int:
        nonlocal probes
        probes += 1
        return await real_depth(queue, max_depth)

    store.queue_depth = counting  # type: ignore[method-assign]
    gated = CairnQ(store, max_queue_depth=1_000)
    try:
        for i in range(20):
            await gated.submit("job", {"i": i})
        # One probe's grant covers the run; the check is not a read per submit.
        assert probes == 1
    finally:
        await gated.close()


async def test_shares_one_in_flight_probe_across_concurrent_submits(db_path):
    store = SQLiteStore(db_path)
    await store.connect()
    probes = 0
    real_depth = store.queue_depth

    async def slow_counting(queue: str, max_depth: int) -> int:
        nonlocal probes
        probes += 1
        # Hold it open so the others must join rather than start their own.
        await asyncio.sleep(0.02)
        return await real_depth(queue, max_depth)

    store.queue_depth = slow_counting  # type: ignore[method-assign]
    gated = CairnQ(store, max_queue_depth=1_000)
    try:
        await asyncio.gather(*(gated.submit("job", {"i": i}) for i in range(8)))
        assert probes == 1
        assert await store.queue_depth("default", 100) == 92
    finally:
        await gated.close()


async def test_gates_task_context_submit_too(client, db_path):
    # "blocker" has no handler on this worker, so it is never claimed and holds
    # the queue at its limit for the whole test — no timing luck involved.
    await client.submit("parent", {})
    await client.submit("blocker", {})

    worker = Worker(
        SQLiteStore(db_path),
        ["default"],
        concurrency=1,
        poll_interval_ms=10,
        max_queue_depth=1,
        max_queue_wait_ms=100,
        queue_poll_interval_ms=20,
    )
    caught: list[BaseException] = []

    async def handler(ctx, payload):
        try:
            await ctx.submit("child", {})
        except BaseException as exc:  # noqa: BLE001 - recorded for the assertion
            caught.append(exc)
        return {}

    worker.register("parent", handler)
    runner = asyncio.ensure_future(worker.run())
    try:
        await wait_for(lambda: bool(caught), 3.0)
        assert caught and isinstance(caught[0], QueueFull)
        # Gating the client alone would have let this through: a worker process
        # has no CairnQ handle, so the fan-out path would be the unbounded one.
        assert await client.list(name="child") == []
    finally:
        worker.stop()
        await runner
        await worker.close()


def test_refuses_a_limit_below_one_at_construction(db_path):
    store = SQLiteStore(db_path)
    with pytest.raises(ValueError, match=">= 1"):
        QueueDepthGate(store, 0)
    with pytest.raises(ValueError, match=">= 1"):
        QueueDepthGate(store, {"q": -1})


# --------------------------------------------------------- max_in_flight_bytes
async def test_holds_back_claims_on_resident_bytes(client, db_path):
    # Four tasks, each ~64KB, against a budget of 100KB: the byte ceiling binds
    # before the concurrency one does, so not all four can run at once.
    big = "x" * (64 * 1024)
    for i in range(4):
        await client.submit("job", {"i": i, "big": big})

    worker = Worker(
        SQLiteStore(db_path),
        ["default"],
        concurrency=4,
        claim_batch=1,  # one task per claim, so the budget is consulted between them
        poll_interval_ms=10,
        max_in_flight_bytes=100 * 1024,
    )
    peak = 0
    in_flight = 0
    gate = asyncio.Event()

    async def handler(ctx, payload):
        nonlocal peak, in_flight
        in_flight += 1
        peak = max(peak, in_flight)
        await gate.wait()
        in_flight -= 1
        return {}

    worker.register("job", handler)
    runner = asyncio.ensure_future(worker.run())
    try:
        await wait_for(lambda: in_flight >= 1)
        await asyncio.sleep(0.12)  # give the loop every chance to over-claim
        # Two 64KB payloads already exceed 100KB, so the third cannot be claimed.
        assert 1 <= peak <= 2
    finally:
        gate.set()
        worker.stop()
        await runner
        await worker.close()


async def test_runs_a_payload_larger_than_the_budget_rather_than_deadlocking(
    client, db_path
):
    # The budget is spent the moment this is charged, so nothing claims alongside
    # it — but refusing to run it at all would stall the queue forever.
    await client.submit("job", {"big": "x" * (200 * 1024)})
    worker = Worker(
        SQLiteStore(db_path),
        ["default"],
        concurrency=2,
        poll_interval_ms=10,
        max_in_flight_bytes=10 * 1024,
    )
    ran = False

    async def handler(ctx, payload):
        nonlocal ran
        ran = True
        return {"ok": True}

    worker.register("job", handler)
    runner = asyncio.ensure_future(worker.run())
    try:
        await wait_for(lambda: ran, 3.0)
        assert ran
    finally:
        worker.stop()
        await runner
        await worker.close()


async def test_refunds_the_charge_so_later_tasks_are_still_claimed(client, db_path):
    big = "x" * (64 * 1024)
    for i in range(3):
        await client.submit("job", {"i": i, "big": big})
    worker = Worker(
        SQLiteStore(db_path),
        ["default"],
        concurrency=1,
        poll_interval_ms=10,
        max_in_flight_bytes=80 * 1024,
    )
    done = 0

    async def handler(ctx, payload):
        nonlocal done
        done += 1
        return {}

    worker.register("job", handler)
    runner = asyncio.ensure_future(worker.run())
    try:
        # Without the refund the budget stays spent after the first task and the
        # remaining two are never claimed.
        await wait_for(lambda: done == 3, 5.0)
        assert done == 3
    finally:
        worker.stop()
        await runner
        await worker.close()


def test_worker_rejects_a_non_positive_byte_budget(db_path):
    with pytest.raises(ValueError, match="> 0"):
        Worker(SQLiteStore(db_path), ["default"], max_in_flight_bytes=0)
