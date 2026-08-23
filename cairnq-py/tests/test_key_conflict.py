"""What a key does when the task it points at has already finished.

The conformance scenarios pin the single-caller branches (key_reuse_terminal,
key_reuse_succeeded, key_reuse_failed). What they cannot express is the reason
the branches live inside the keyed transaction at all: concurrent submits.
Deciding "is this task still live?" outside it — read the status, then pick a
strategy — puts an await between the read and the write, and two callers that
both read the same finished task both replace, each cancelling the other's fresh
task. That is the double-submit a key exists to prevent.
"""

from __future__ import annotations

import asyncio

import pytest

from cairnq import AlreadyExists, CairnQ


async def _finish(client: CairnQ, task_id: str, result: dict | None = None) -> None:
    """Run a task to `succeeded`, the way a worker would."""
    await client.store.claim(queues=["default"], worker_id="w1", lease_ms=5_000)
    await client.store.succeed(task_id=task_id, worker_id="w1", result=result or {"n": 1})


# Both dialects: the key lock this suite is really about is where they differ
# most — a no-op on SQLite, where BEGIN IMMEDIATE already serializes every keyed
# transaction, and a pg_advisory_xact_lock on Postgres, where READ COMMITTED
# gives two concurrent same-key submits nothing to serialize on. The concurrent
# case below is the one that would silently pass on SQLite while the Postgres
# lock was missing or wrong. Mirrors cairnq-node/test/key-conflict.test.ts.
async def test_a_failed_task_frees_the_key(backend):
    client = await backend.client()
    first = await client.submit("job", {}, key="A", max_attempts=1)
    await client.store.claim(queues=["default"], worker_id="w1", lease_ms=5_000)
    await client.store.fail(
        task_id=first.id,
        worker_id="w1",
        error={"type": "E", "code": "boom", "message": "boom", "retryable": True},
        retryable=True,
        delay_ms=0,
    )

    second = await client.submit("job", {}, key="A", max_attempts=1)
    assert second.id != first.id
    assert second.status == "queued"
    # The failed task is left as it was: nothing re-cancels a settled row.
    assert (await client.get(first.id)).status == "failed"


async def test_a_succeeded_result_comes_back_only_under_reuse_succeeded(backend):
    client = await backend.client()
    first = await client.submit("job", {}, key="A")
    await _finish(client, first.id)

    cached = await client.submit("job", {}, key="A", conflict="reuse-succeeded")
    assert cached.id == first.id
    assert cached.result == {"n": 1}

    # …and reuse-succeeded repointed nothing, so plain reuse still starts over.
    rerun = await client.submit("job", {}, key="A")
    assert rerun.id != first.id
    assert rerun.status == "queued"


async def test_concurrent_submits_collapse_onto_one_task(backend):
    client = await backend.client()
    first = await client.submit("job", {}, key="A")
    await _finish(client, first.id)

    # The double-click, arriving twice at once against a key whose last task
    # finished. Whoever loses the race sees the winner's fresh task — queued, so
    # reusable — rather than starting a second one.
    racers = await asyncio.gather(*(client.submit("job", {}, key="A") for _ in range(8)))
    ids = {t.id for t in racers}
    assert len(ids) == 1
    assert first.id not in ids

    current = await client.get_by_key("A")
    assert current.id == racers[0].id
    assert current.status == "queued"
    # No task was created and then thrown away: two rows exist, the finished one
    # and the new one.
    assert len(await client.list()) == 2


async def test_reject_still_rejects_a_finished_task(backend):
    client = await backend.client()
    first = await client.submit("job", {}, key="A", conflict="reject")
    await _finish(client, first.id)
    # reject asks for a key that is used at most once, ever — a task reaching a
    # terminal state does not make the key free again.
    with pytest.raises(AlreadyExists):
        await client.submit("job", {}, key="A", conflict="reject")


async def test_replace_still_cancels_a_live_task(backend):
    client = await backend.client()
    first = await client.submit("job", {}, key="A")
    second = await client.submit("job", {}, key="A", conflict="replace")
    assert second.id != first.id
    assert (await client.get(first.id)).status == "canceled"
