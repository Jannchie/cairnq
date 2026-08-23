"""Batch delivery: one handler call over several tasks.

The contract under test is single — **when a batch handler returns, every task it
did not settle itself is settled by how the call ended** — plus the escape hatch
that makes it usable: a handler can settle individual tasks as it goes, and the
worker neither re-settles those nor keeps renewing their leases.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from cairnq import LostLease, TaskError, Worker

from .helpers import all_terminal, wait_for


async def _drain(client, ids, worker):
    """Run the worker until every id is terminal, then return the tasks by id."""
    async with worker.background():
        await wait_for(lambda: all_terminal(client, ids), timeout_s=5.0)
    return {i: await client.get(i) for i in ids}


# Both dialects: a batched worker draws with the one-queue/one-name claim
# specialisations, and those are four separate statements per dialect. A batch
# that silently came back short on one of them would look like a quiet queue.
async def test_a_batch_handler_is_called_once_for_the_whole_batch(backend):
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=8)
    calls: list[int] = []

    @worker.task("embed", batch=8)
    async def embed(items):
        calls.append(len(items))
        return {item.task_id: {"n": item.payload["n"]} for item in items}

    ids = [(await client.submit("embed", {"n": n})).id for n in range(5)]
    tasks = await _drain(client, ids, worker)

    assert calls == [5]
    assert all(t.status == "succeeded" for t in tasks.values())
    # The returned mapping fills in each task's own result.
    assert {t.result["n"] for t in tasks.values()} == {0, 1, 2, 3, 4}


async def test_a_batch_is_chunked_by_its_registered_size(backend):
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=8)
    calls: list[int] = []

    @worker.task("embed", batch=3)
    async def embed(items):
        calls.append(len(items))

    ids = [(await client.submit("embed", {"n": n})).id for n in range(7)]
    tasks = await _drain(client, ids, worker)

    # 7 tasks, size 3 -> no call ever exceeds the registered size.
    assert sum(calls) == 7
    assert max(calls) <= 3
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_returning_nothing_succeeds_the_whole_batch_with_no_result(backend):
    """The common shape: the handler wrote its output to a database, so there is
    no per-task result to carry back."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=4)

    @worker.task("index", batch=4)
    async def index(items):
        return None

    ids = [(await client.submit("index", {})).id for _ in range(3)]
    tasks = await _drain(client, ids, worker)

    assert all(t.status == "succeeded" for t in tasks.values())
    assert all(t.result is None for t in tasks.values())


async def test_raising_fails_every_unsettled_task_retryably(backend):
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=4, retry_backoff_ms=0)

    @worker.task("flaky", batch=4)
    async def flaky(items):
        raise RuntimeError("provider down")

    ids = [(await client.submit("flaky", {}, max_attempts=1)).id for _ in range(3)]
    tasks = await _drain(client, ids, worker)

    assert all(t.status == "failed" for t in tasks.values())
    # Each task keeps its own error record and its own attempt count.
    assert all(t.error["message"] == "provider down" for t in tasks.values())
    assert all(t.attempt == 1 for t in tasks.values())


async def test_a_retryable_batch_failure_is_re_attempted_per_task(backend):
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=4, retry_backoff_ms=0)
    attempts: list[int] = []

    @worker.task("flaky", batch=4)
    async def flaky(items):
        attempts.append(len(items))
        if len(attempts) == 1:
            raise RuntimeError("transient")

    ids = [(await client.submit("flaky", {}, max_attempts=3)).id for _ in range(2)]
    tasks = await _drain(client, ids, worker)

    assert all(t.status == "succeeded" for t in tasks.values())
    assert all(t.attempt == 2 for t in tasks.values())


async def test_raising_a_non_retryable_TaskError_fails_the_rest_permanently(backend):
    """The `abort_for_credit_depletion` shape: one condition ends the whole batch
    and nothing should be retried."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=4, retry_backoff_ms=0)

    @worker.task("translate", batch=4)
    async def translate(items):
        raise TaskError("credit depleted", code="credit_depleted", retryable=False)

    ids = [(await client.submit("translate", {}, max_attempts=5)).id for _ in range(3)]
    tasks = await _drain(client, ids, worker)

    assert all(t.status == "failed" for t in tasks.values())
    assert all(t.error["code"] == "credit_depleted" for t in tasks.values())
    # Permanent: one attempt, despite max_attempts=5.
    assert all(t.attempt == 1 for t in tasks.values())


async def test_a_handler_can_settle_some_tasks_and_let_the_rest_ride(backend):
    """The real shape from a production batch handler: a few tasks fail for their
    own reasons, the rest succeed by the handler simply returning."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=8, retry_backoff_ms=0)

    @worker.task("embed", batch=8)
    async def embed(items):
        for item in items:
            if item.payload["n"] % 2:
                await item.fail("odd is not embeddable", retryable=False)
        return {item.task_id: {"n": item.payload["n"]} for item in items}

    ids = [(await client.submit("embed", {"n": n}, max_attempts=3)).id for n in range(6)]
    tasks = await _drain(client, ids, worker)

    by_n = {t.payload["n"]: t for t in tasks.values()}
    assert [by_n[n].status for n in range(6)] == [
        "succeeded", "failed", "succeeded", "failed", "succeeded", "failed",
    ]
    # The explicitly failed ones kept their own reason and were not retried,
    # even though the handler returned normally and max_attempts allowed more.
    assert by_n[1].error["message"] == "odd is not embeddable"
    assert by_n[1].attempt == 1
    # A task the handler settled is not overwritten by the batch's return value.
    assert by_n[1].result is None
    assert by_n[0].result == {"n": 0}


async def test_settling_twice_is_a_no_op(backend):
    """Handlers built on ack/nack queues all carry a `finalized_ids` set to
    guarantee this. Holding it in the context is the point."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=4)

    @worker.task("once", batch=4)
    async def once(items):
        for item in items:
            assert await item.succeed({"first": True}) is not None
            assert item.settled
            # Every later attempt is a no-op that reports it did nothing.
            assert await item.succeed({"second": True}) is None
            assert await item.fail("too late") is None

    ids = [(await client.submit("once", {})).id for _ in range(2)]
    tasks = await _drain(client, ids, worker)

    assert all(t.status == "succeeded" for t in tasks.values())
    assert all(t.result == {"first": True} for t in tasks.values())


async def test_an_explicitly_failed_task_is_retried_when_retryable(backend):
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, retry_backoff_ms=0)
    seen: list[int] = []

    @worker.task("retryable", batch=4)
    async def retryable(items):
        for item in items:
            seen.append(item.attempt)
            if item.attempt == 1:
                await item.fail("not yet", retryable=True)

    ids = [(await client.submit("retryable", {}, max_attempts=3)).id]
    tasks = await _drain(client, ids, worker)

    assert seen == [1, 2]
    assert tasks[ids[0]].status == "succeeded"


async def test_batch_and_single_handlers_share_one_worker(backend):
    """A claim comes back mixed by name; each name is delivered its own way."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=8)
    batched: list[int] = []
    singles: list[str] = []

    @worker.task("embed", batch=8)
    async def embed(items):
        batched.append(len(items))

    @worker.task("summarize")
    async def summarize(ctx, payload):
        singles.append(ctx.task_id)
        return {"ok": True}

    ids = [(await client.submit("embed", {})).id for _ in range(4)]
    ids += [(await client.submit("summarize", {})).id for _ in range(2)]
    tasks = await _drain(client, ids, worker)

    assert sum(batched) == 4
    assert len(singles) == 2
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_batch_of_one_is_still_a_batch_call(backend):
    """batch=1 is a real configuration — work that saturates the machine (a
    Docling parse) is registered this way, and must still get the list form."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20)
    shapes: list[int] = []

    @worker.task("parse", batch=1)
    async def parse(items):
        shapes.append(len(items))

    ids = [(await client.submit("parse", {})).id for _ in range(3)]
    tasks = await _drain(client, ids, worker)

    assert shapes == [1, 1, 1]
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_the_batch_heartbeat_keeps_every_lease_alive(backend):
    """A handler outliving its lease must not have its tasks recovered under it —
    and one beat has to cover the whole batch, not one task at a time."""
    client = await backend.client()
    # Margins, not luck: the point is that ONE beat covers the whole batch, and
    # the handler outlives its lease several times over. A loaded CI runner can
    # delay a beat past a 200ms lease without anything being wrong with the
    # worker, which fails this for a reason it is not testing. Longer lease, same
    # beat-to-lease ratio, same number of lifetimes slept. Mirrors the margins in
    # batch-delivery.test.ts.
    worker = backend.worker(
        poll_interval_ms=20, concurrency=4, lease_ms=1_000
    )

    @worker.task("slow", batch=4)
    async def slow(items):
        await asyncio.sleep(3.0)  # three lease lifetimes
        assert not any(item.lost_lease for item in items)

    ids = [(await client.submit("slow", {}, max_attempts=1)).id for _ in range(3)]
    tasks = await _drain(client, ids, worker)

    assert all(t.status == "succeeded" for t in tasks.values())
    # Never redelivered: a recovered lease would have burned the single attempt.
    assert all(t.attempt == 1 for t in tasks.values())


async def test_a_settled_task_stops_being_heartbeaten(backend):
    """Renewing a lease on a terminal row is a write against something nobody
    owns; the beat has to drop tasks the handler already finished."""
    client = await backend.client()
    # Same margin reasoning as the case above. A lease expiring here does not
    # just delay the test, it changes what it measures: the surviving task gets
    # recovered and redelivered as a batch of its own, whose items[0] settles
    # early too, and the assertion below counts two.
    worker = backend.worker(
        poll_interval_ms=20, concurrency=4, lease_ms=1_000
    )

    @worker.task("half", batch=4)
    async def half(items):
        await items[0].succeed({"early": True})
        await asyncio.sleep(1.5)  # several beats, with one task already terminal

    ids = [(await client.submit("half", {})).id for _ in range(2)]
    tasks = await _drain(client, ids, worker)

    assert all(t.status == "succeeded" for t in tasks.values())
    early = [t for t in tasks.values() if t.result == {"early": True}]
    assert len(early) == 1


async def test_max_run_ms_bounds_the_whole_batch_call(backend):
    client = await backend.client()
    worker = backend.worker(
        poll_interval_ms=20, concurrency=4, max_run_ms=150, retry_backoff_ms=0
    )

    @worker.task("hang", batch=4)
    async def hang(items):
        await asyncio.sleep(10)

    ids = [(await client.submit("hang", {}, max_attempts=1)).id for _ in range(2)]
    tasks = await _drain(client, ids, worker)

    assert all(t.status == "failed" for t in tasks.values())
    assert all(t.error["code"] == "handler_timeout" for t in tasks.values())


async def test_registering_a_non_positive_batch_is_rejected(backend):
    client = await backend.client()
    worker = backend.worker()
    with pytest.raises(ValueError, match="batch must be >= 1"):
        worker.register("x", lambda items: None, batch=0)


async def test_a_cancel_reaches_a_batch_task_through_the_heartbeat(backend):
    """Cancellation rides along on the write the worker was making anyway — in a
    batch that write is the shared beat, so it must carry each row back."""
    client = await backend.client()
    worker = backend.worker(
        poll_interval_ms=20, concurrency=4, lease_ms=400
    )
    observed: dict[str, bool] = {}

    @worker.task("cancellable", batch=4)
    async def cancellable(items):
        # Wait for the cancel to land, then report it without re-reading the row.
        await wait_for(lambda: items[0]._cancel_seen, timeout_s=2.0)
        observed["seen"] = await items[0].canceled()
        await asyncio.sleep(0.05)

    target = await client.submit("cancellable", {})
    async with worker.background():
        await wait_for(lambda: _is_running(client, target.id), timeout_s=2.0)
        await client.cancel(target.id)
        await wait_for(lambda: all_terminal(client, [target.id]), timeout_s=3.0)

    assert observed.get("seen") is True
    assert (await client.get(target.id)).status == "canceled"


async def _is_running(client, task_id) -> bool:
    task = await client.get(task_id)
    return task is not None and task.running


async def test_a_single_task_handler_can_settle_early(backend):
    """succeed()/fail() are on TaskContext, not on anything batch-shaped, so they
    work in single-task delivery too — there they mean "settle now". The worker
    must then not complete the task a second time over the handler's decision."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20)

    @worker.task("early")
    async def early(ctx, payload):
        await ctx.succeed({"decided_by": "handler"})
        return {"decided_by": "return value"}  # ignored: already settled

    ids = [(await client.submit("early", {})).id]
    tasks = await _drain(client, ids, worker)

    assert tasks[ids[0]].status == "succeeded"
    assert tasks[ids[0]].result == {"decided_by": "handler"}


async def test_a_single_task_handler_can_fail_itself_permanently(backend):
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, retry_backoff_ms=0)

    @worker.task("doomed")
    async def doomed(ctx, payload):
        await ctx.fail("bad input", retryable=False)

    ids = [(await client.submit("doomed", {}, max_attempts=5)).id]
    tasks = await _drain(client, ids, worker)

    assert tasks[ids[0]].status == "failed"
    assert tasks[ids[0]].error["message"] == "bad input"
    assert tasks[ids[0]].attempt == 1  # permanent, despite max_attempts=5


async def test_a_batched_name_does_not_start_calls_for_an_unbatched_one(backend):
    """Regression: the claim used to be one statement over every registered name,
    so sizing it for the widest batch let a `batch=64` registration pull 64 rows
    of unrelated work and turn each into its own call on a worker configured for
    one. Each name now draws its own quota."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=1)
    live = 0
    peak = 0

    @worker.task("embed", batch=64)
    async def embed(items):
        pass

    @worker.task("solo")
    async def solo(ctx, payload):
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.02)
        live -= 1

    ids = [(await client.submit("solo", {})).id for _ in range(20)]
    tasks = await _drain(client, ids, worker)

    assert peak == 1, f"concurrency=1 but {peak} handlers ran at once"
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_concurrency_bounds_calls_not_tasks(backend):
    """concurrency counts handler calls: a call holding 4 tasks is one of them.
    Counting tasks instead is what used to weld batch size to concurrency — a
    full batch was unreachable unless concurrency was raised to match it."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20, concurrency=2)
    calls = 0
    peak_calls = 0
    widest = 0

    @worker.task("embed", batch=4)
    async def embed(items):
        nonlocal calls, peak_calls, widest
        calls += 1
        peak_calls = max(peak_calls, calls)
        widest = max(widest, len(items))
        await asyncio.sleep(0.03)
        calls -= 1

    ids = [(await client.submit("embed", {})).id for _ in range(20)]
    tasks = await _drain(client, ids, worker)

    assert peak_calls <= 2, f"concurrency=2 but {peak_calls} calls ran at once"
    # A full batch is reachable at concurrency 2 — 8 tasks in flight, 2 calls.
    assert widest == 4
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_a_batch_fills_at_the_default_concurrency(backend):
    """The headline of the change: batch size is no longer capped by concurrency,
    so `batch=8` on a default worker delivers 8 rather than 1."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20)
    sizes: list[int] = []

    @worker.task("embed", batch=8)
    async def embed(items):
        sizes.append(len(items))

    ids = [(await client.submit("embed", {"n": n})).id for n in range(8)]
    tasks = await _drain(client, ids, worker)

    assert sizes == [8]
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_a_name_is_not_starved_behind_another_names_backlog(backend):
    """One slot, two backlogs. The claim serves groups in the order given, so
    without rotating that order `embed` would hold the slot until its 40 tasks
    were done and `other` would not run at all."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=5, concurrency=1)
    done = {"embed": 0, "other": 0}

    @worker.task("embed", batch=4)
    async def embed(items):
        done["embed"] += len(items)

    @worker.task("other", batch=4)
    async def other(items):
        done["other"] += len(items)

    ids = [(await client.submit("embed", {})).id for _ in range(40)]
    ids += [(await client.submit("other", {})).id for _ in range(8)]
    await _drain(client, ids, worker)

    assert done["embed"] == 40
    # The real assertion is that this finished at all — a starved name would
    # leave _drain to time out.
    assert done["other"] == 8


async def test_settling_during_a_beat_is_not_read_as_lease_loss(backend):
    """Regression: the beat renews only rows still `running`, so a task the
    handler settled while the beat was in flight comes back absent — which the
    loop read as "another worker took it" and flagged the context lease-lost.
    A handler checking `lost_lease` was told to bail out after a clean succeed."""
    client = await backend.client()
    worker = backend.worker(
        poll_interval_ms=20, concurrency=4, lease_ms=300
    )
    flagged: dict[str, list[bool]] = {}

    @worker.task("racy", batch=4)
    async def racy(items):
        await asyncio.sleep(0.15)  # let a beat land first (lease/3 = 100ms)
        for item in items:
            await item.succeed({"ok": True})
        await asyncio.sleep(0.3)  # and several more beats after settling
        flagged["lost"] = [i.lost_lease for i in items]

    ids = [(await client.submit("racy", {})).id for _ in range(3)]
    tasks = await _drain(client, ids, worker)

    assert flagged["lost"] == [False, False, False]
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_a_single_task_handler_that_settles_early_is_not_flagged_either(backend):
    """The same rule through the single-task path, which shares the loop: it used
    to heartbeat a terminal row every beat and flag the context on the first one."""
    client = await backend.client()
    worker = backend.worker(
        poll_interval_ms=20, lease_ms=300
    )
    flagged: dict[str, bool] = {}

    @worker.task("early")
    async def early(ctx, payload):
        await ctx.succeed({"ok": True})
        await asyncio.sleep(0.3)  # several beats with the task already terminal
        flagged["lost"] = ctx.lost_lease

    ids = [(await client.submit("early", {})).id]
    tasks = await _drain(client, ids, worker)

    assert flagged["lost"] is False
    assert tasks[ids[0]].status == "succeeded"


async def test_writing_after_settling_does_not_look_like_a_lost_lease(backend):
    """`settled` gates every write through the context, not just the settlement
    ones. Without that, `progress()` after a `succeed()` reaches the store, fails
    the ownership check on a terminal row, and reports LostLease — telling the
    handler another worker took its task when it had simply already finished it,
    and flipping `lost_lease` on the way out."""
    client = await backend.client()
    worker = backend.worker(poll_interval_ms=20)
    seen: dict[str, Any] = {}

    @worker.task("early")
    async def early(ctx, payload):
        await ctx.succeed({"ok": True})
        try:
            await ctx.progress(0.5, "too late")
        except LostLease:
            seen["raised"] = True
        seen["lost_lease"] = ctx.lost_lease

    ids = [(await client.submit("early", {})).id]
    tasks = await _drain(client, ids, worker)

    assert seen["raised"] is True  # the write is refused, as before
    assert seen["lost_lease"] is False  # but the lease state is not corrupted
    assert tasks[ids[0]].status == "succeeded"
