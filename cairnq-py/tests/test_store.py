import asyncio

import pytest

from cairnq.errors import LostLease


async def test_protocol_version(client):
    assert await client.store.protocol_version() == 1


async def test_stats_on_an_empty_database(client):
    # No rows, no queues — {} rather than a zero-filled "default" that would
    # imply the store knows which queues exist before anything is submitted.
    assert await client.stats() == {}


async def test_protocol_version_mismatch(tmp_path):
    import sqlite3

    from cairnq import CairnQ
    from cairnq.errors import ProtocolVersionMismatch

    db = str(tmp_path / "bad.db")
    conn = sqlite3.connect(db)
    conn.execute("create table cairnq_meta (key text primary key, value text not null)")
    conn.execute("insert into cairnq_meta values ('protocol_version', '999')")
    conn.commit()
    conn.close()

    bad = CairnQ.sqlite(db)
    with pytest.raises(ProtocolVersionMismatch):
        await bad.connect()
    await bad.close()


async def test_claim_priority_then_fifo(client):
    store = client.store
    a = await client.submit("job", {}, priority=0)
    await asyncio.sleep(0.002)
    b = await client.submit("job", {}, priority=5)
    await asyncio.sleep(0.002)
    c = await client.submit("job", {}, priority=0)

    first = await store.claim(queues=["default"], worker_id="w", lease_ms=5000, limit=1)
    assert first[0].id == b.id  # highest priority wins
    rest = await store.claim(queues=["default"], worker_id="w", lease_ms=5000, limit=5)
    assert [t.id for t in rest] == [a.id, c.id]  # then FIFO by created_at


async def test_claim_batch_limit(client):
    store = client.store
    for _ in range(5):
        await client.submit("job", {})
    claimed = await store.claim(queues=["default"], worker_id="w", lease_ms=5000, limit=3)
    assert len(claimed) == 3
    assert all(t.status == "running" and t.attempt == 1 for t in claimed)


async def test_queue_isolation(client):
    store = client.store
    await client.submit("job", {}, queue="gpu")
    await client.submit("job", {}, queue="io")
    claimed = await store.claim(queues=["gpu"], worker_id="w", lease_ms=5000, limit=10)
    assert len(claimed) == 1 and claimed[0].queue == "gpu"


@pytest.mark.parametrize("method", ["heartbeat", "progress", "succeed", "complete", "fail"])
async def test_worker_writes_are_ownership_checked(client, method):
    store = client.store
    t = await client.submit("job", {})
    await store.claim(queues=["default"], worker_id="owner", lease_ms=5000)
    with pytest.raises(LostLease):
        if method == "heartbeat":
            await store.heartbeat(task_id=t.id, worker_id="intruder", lease_ms=5000)
        elif method == "progress":
            await store.progress(task_id=t.id, worker_id="intruder", progress=0.5, message=None)
        elif method == "succeed":
            await store.succeed(task_id=t.id, worker_id="intruder", result={})
        elif method == "complete":
            await store.complete(task_id=t.id, worker_id="intruder", result={})
        else:
            await store.fail(task_id=t.id, worker_id="intruder", error={"code": "x"})


async def test_lease_recovery_requeues(client):
    store = client.store
    t = await client.submit("job", {}, max_attempts=3)
    c1 = await store.claim(queues=["default"], worker_id="w1", lease_ms=100)
    assert len(c1) == 1
    await asyncio.sleep(0.2)
    c2 = await store.claim(queues=["default"], worker_id="w2", lease_ms=5000)
    assert len(c2) == 1 and c2[0].id == t.id and c2[0].attempt == 2 and c2[0].worker_id == "w2"


async def test_lease_recovery_final_failure(client):
    store = client.store
    t = await client.submit("job", {}, max_attempts=1)
    c1 = await store.claim(queues=["default"], worker_id="w1", lease_ms=100)
    assert len(c1) == 1
    await asyncio.sleep(0.2)
    c2 = await store.claim(queues=["default"], worker_id="w2", lease_ms=5000)
    assert c2 == []  # not reclaimable: attempt reached max_attempts
    got = await client.get(t.id)
    assert got.status == "failed" and got.error["code"] == "lease_expired"


async def test_cancel_running_is_cooperative(client):
    store = client.store
    t = await client.submit("job", {})
    await store.claim(queues=["default"], worker_id="w1", lease_ms=5000)
    canceled = await client.cancel(t.id)
    assert canceled.status == "running"  # not force-killed
    got = await client.get(t.id)
    assert got.status == "running" and got.cancel_requested


async def test_retry_failed_task(client):
    store = client.store
    t = await client.submit("job", {}, max_attempts=1)
    await store.claim(queues=["default"], worker_id="w1", lease_ms=5000)
    await store.fail(task_id=t.id, worker_id="w1", error={"code": "boom"}, retryable=True)
    failed = await client.get(t.id)
    assert failed.status == "failed"
    retried = await client.retry(t.id, reset_attempt=True)
    assert retried.status == "queued" and retried.attempt == 0


async def test_list_filters(client):
    a = await client.submit("alpha", {}, correlation_id="req-1")
    await client.submit("beta", {}, correlation_id="req-2")
    by_name = await client.list(name="alpha")
    assert [t.id for t in by_name] == [a.id]
    by_corr = await client.list(correlation_id="req-1")
    assert [t.id for t in by_corr] == [a.id]


async def test_concurrent_claims_never_double_dispatch(client):
    # Fire more concurrent single-claims than there are tasks: each must be claimed
    # exactly once. The SQLite store serializes writes behind one connection/lock,
    # so this passes trivially here; its teeth are when the same assertion runs
    # against the Postgres backend (FOR UPDATE SKIP LOCKED) under real contention.
    store = client.store
    n = 10
    for i in range(n):
        await client.submit("job", {"i": i})
    batches = await asyncio.gather(
        *(
            store.claim(queues=["default"], worker_id=f"w{k}", lease_ms=5000, limit=1)
            for k in range(n + 5)
        )
    )
    ids = [t.id for batch in batches for t in batch]
    assert len(ids) == n  # exactly n dispatched, no phantom claims
    assert len(set(ids)) == n  # and never the same task twice
