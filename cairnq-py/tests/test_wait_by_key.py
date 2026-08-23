"""Picking a wait back up after it timed out.

`wait_timeout_ms` bounds the wait, not the task — the task runs on. Getting at
that result afterwards used to mean submitting again under the key and hoping the
conflict strategy handed the finished task back, which is a re-submit dressed as
a read. `wait(err.task_id)` and `wait_by_key(key)` make it a read.
"""

from __future__ import annotations

import asyncio

import pytest

from cairnq import CairnQ, TaskTimeout


async def _finish_next(client: CairnQ, result: dict) -> None:
    """Finish a claimed task out of band, the way a worker elsewhere would."""
    (task,) = await client.store.claim(queues=["default"], worker_id="w1", lease_ms=5_000)
    await client.store.succeed(task_id=task.id, worker_id="w1", result=result)


# Both dialects: a key resolves through get_status_by_key on every probe, and
# that statement is written separately per dialect. A wait that stops following
# the key on one of them looks identical from the API side — it just never
# finishes.
async def test_reattaches_by_id_without_rerunning(backend):
    client = await backend.client()
    with pytest.raises(TaskTimeout) as caught:
        await client.call("job", {}, key="A", wait_timeout_ms=50)
    err = caught.value
    assert err.task_id
    assert err.key is None

    await _finish_next(client, {"n": 1})
    done = await client.wait(err.task_id, timeout_ms=2_000)
    assert done.status == "succeeded"
    assert done.result == {"n": 1}
    # One task, one run: the second wait read the store, it did not submit.
    assert len(await client.list()) == 1


async def test_reattaches_by_key(backend):
    client = await backend.client()
    await client.submit("job", {}, key="A")
    waiting = asyncio.create_task(client.wait_by_key("A", timeout_ms=2_000))
    await _finish_next(client, {"n": 2})
    assert (await waiting).result == {"n": 2}


async def test_follows_the_key_onto_a_replacement(backend):
    client = await backend.client()
    # A key points at whichever task is current, so a `replace` landing mid-wait
    # moves the wait rather than reporting the old task's cancellation.
    first = await client.submit("job", {}, key="A")
    waiting = asyncio.create_task(client.wait_by_key("A", timeout_ms=3_000))
    await asyncio.sleep(0.03)
    second = await client.submit("job", {}, key="A", conflict="replace")
    assert (await client.get(first.id)).status == "canceled"

    await _finish_next(client, {"n": 3})
    done = await waiting
    assert done.id == second.id
    assert done.result == {"n": 3}


async def test_waits_for_a_key_that_points_at_nothing_yet(backend):
    client = await backend.client()
    waiting = asyncio.create_task(client.wait_by_key("A", timeout_ms=3_000))
    await asyncio.sleep(0.03)
    await client.submit("job", {}, key="A")
    await _finish_next(client, {"n": 4})
    assert (await waiting).result == {"n": 4}


async def test_names_the_key_it_was_watching(backend):
    client = await backend.client()
    with pytest.raises(TaskTimeout) as caught:
        await client.wait_by_key("missing", timeout_ms=30)
    err = caught.value
    assert err.key == "missing"
    assert "key missing" in str(err)
    assert "no task under this key" in str(err)
