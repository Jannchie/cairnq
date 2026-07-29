"""Regression tests for edge cases the library used to mishandle: non-JSON
values crossing the protocol boundary, tasks stranded by unserializable results,
sub-second leases outrun by the heartbeat floor, and silent typo'd list filters.
The TypeScript twin is cairnq-node/test/edge-cases.test.ts.
"""

from __future__ import annotations

import asyncio

import pytest

from cairnq import SerializationError, TaskError, Worker
from cairnq.errors import TaskFailed


async def test_non_finite_floats_are_rejected_at_submit(client):
    # json.dumps would otherwise write bare NaN/Infinity — not JSON — which the
    # TypeScript SDK's JSON.parse throws on, poisoning the row for every
    # cross-language reader.
    with pytest.raises(SerializationError):
        await client.submit("job", {"x": float("nan")})
    with pytest.raises(SerializationError):
        await client.submit("job", {"x": 1.0}, metadata={"y": float("inf")})


async def test_unserializable_result_fails_the_task_promptly(client, db_path):
    # The failure is deterministic, so it must be recorded as a permanent
    # SerializationError on the first attempt — not strand the task `running`
    # until lease expiry redelivers it to fail the same way again.
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20, lease_ms=600)
    runs = 0

    @worker.task("bad-result")
    async def handle(ctx, payload):
        nonlocal runs
        runs += 1
        return {"vals": {1, 2}}  # a set is not JSON-serializable

    async with worker.background():
        task = await client.submit("bad-result", {})
        # Well under the first lease expiry: no redelivery may be involved.
        final = await client.wait(task.id, timeout_ms=500, poll_ms=20)

    assert final.failed
    assert final.error["code"] == "unserializable_result"
    assert runs == 1


async def test_unserializable_task_error_details_still_record_the_failure(client, db_path):
    # A TaskError carrying exotic details must not strand the task: the envelope
    # is stripped to its string fields and the failure is recorded.
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20)

    @worker.task("bad-details")
    async def handle(ctx, payload):
        raise TaskError("boom", details={"weird": {1, 2}})

    async with worker.background():
        with pytest.raises(TaskFailed) as excinfo:
            await client.call("bad-details", {}, wait_timeout_ms=3_000, poll_ms=20)

    assert excinfo.value.message == "boom"
    assert excinfo.value.details == {}


async def test_sub_second_lease_is_maintained_by_the_heartbeat(client, db_path):
    # The heartbeat floor used to be 1s, so a lease below that could never be
    # maintained: the worker's own claim loop recovered the "expired" lease and
    # re-ran the task while the first attempt was still going.
    worker = Worker.sqlite(
        db_path, queues=["default"], poll_interval_ms=20, lease_ms=200, concurrency=2
    )
    runs = 0

    @worker.task("slow")
    async def handle(ctx, payload):
        nonlocal runs
        runs += 1
        await asyncio.sleep(0.4)  # outlives two lease periods
        return {"ok": True}

    async with worker.background():
        result = await client.call("slow", {}, wait_timeout_ms=3_000, poll_ms=20)

    assert result == {"ok": True}
    assert runs == 1


async def test_list_rejects_an_unknown_status(client):
    # A typo'd status used to match nothing and return [] indistinguishably
    # from "no such tasks".
    with pytest.raises(ValueError):
        await client.list(status="succeded")  # typo on purpose
