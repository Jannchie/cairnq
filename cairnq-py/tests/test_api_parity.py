"""Places where the Python SDK could not express something the TypeScript SDK
could. Each of these was reachable from `tasks.submit(...)` / `Worker.sqlite(...)`
in TS and simply missing here."""

from __future__ import annotations

import asyncio

import pytest

from cairnq import CairnQ, Worker


def test_worker_sqlite_forwards_store_options(tmp_path):
    """Worker.sqlite(**kwargs) sent everything to the Worker, so there was no way
    to set a store option like busy_timeout_ms from the worker entry point."""
    worker = Worker.sqlite(str(tmp_path / "t.db"), busy_timeout_ms=1_234)
    assert worker._store._busy_timeout_ms == 1_234


async def test_submit_accepts_an_explicit_parent(client):
    """TaskContext.submit wired parent/root automatically, but a caller outside a
    handler (an API process resuming a chain) had no way to say so."""
    parent = await client.submit("parent", {})
    child = await client.submit(
        "child", {}, parent_id=parent.id, root_id=parent.root_id
    )
    assert child.parent_id == parent.id
    assert child.root_id == parent.root_id

    chain = await client.list(root_id=parent.root_id)
    assert {t.id for t in chain} == {parent.id, child.id}


async def test_list_rejects_an_unknown_filter(client):
    """list(**filters) forwarded anything, so a typo'd filter reached the store as
    a TypeError from deep inside instead of at the call."""
    with pytest.raises(TypeError):
        await client.list(nmae="typo")


async def test_purge_is_available_on_the_client(client, db_path):
    t = await client.submit("job", {})
    store = client.store
    (claimed,) = await store.claim(queues=["default"], worker_id="w1", lease_ms=5_000)
    await store.succeed(task_id=t.id, worker_id="w1", result={"ok": True})

    assert await client.purge(older_than_ms=3_600_000) == []
    # purge's cutoff is strict (`completed_at_ms < :before_ms`), so a task that
    # finished in the CURRENT millisecond is not yet "older than 0ms ago". On a
    # fast machine the succeed above lands in the same millisecond as the purge
    # below; one tick of the clock is what the assertion is actually waiting for.
    await asyncio.sleep(0.005)
    assert await client.purge(older_than_ms=0) == [t.id]
    assert await client.get(t.id) is None
