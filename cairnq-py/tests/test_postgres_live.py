"""Live Postgres smoke. Skipped unless CAIRNQ_TEST_PG_DSN is set — CI provides a
`postgres` service; locally point it at any throwaway database. Runs the paths
static review can't prove out: real asyncpg type inference on NULL/jsonb params,
FOR UPDATE SKIP LOCKED under genuine concurrency, the JSON round-trip, DB-clock
lease recovery, and the *_by_key transactions."""

import asyncio
import os

import pytest

from cairnq import CairnQ, Worker
from cairnq.errors import LostLease

DSN = os.environ.get("CAIRNQ_TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set CAIRNQ_TEST_PG_DSN to run live PG tests")


@pytest.fixture
async def pg_client():
    import asyncpg

    client = CairnQ.postgres(DSN)
    await client.connect()  # applies migrations (idempotent)
    admin = await asyncpg.connect(DSN)
    await admin.execute("truncate cairnq_tasks, cairnq_task_keys")
    await admin.close()
    try:
        yield client
    finally:
        await client.close()


async def test_json_round_trip(pg_client):
    payload = {"a": None, "nested": {"x": [1, 2, 3]}, "u": "café — 日本語", "empty": {}}
    t = await pg_client.submit("job", payload)
    assert (await pg_client.get(t.id)).payload == payload

    store = pg_client.store
    (c,) = await store.claim(queues=["default"], worker_id="w1", lease_ms=5000)
    assert c.id == t.id
    await store.succeed(task_id=t.id, worker_id="w1", result={"out": [None, {"k": "ü"}]})
    done = await pg_client.get(t.id)
    assert done.status == "succeeded" and done.result == {"out": [None, {"k": "ü"}]}


async def test_null_params(pg_client):
    store = pg_client.store
    t = await pg_client.submit("job", {})
    await store.claim(queues=["default"], worker_id="w1", lease_ms=5000)
    p = await store.progress(task_id=t.id, worker_id="w1", progress=0.5, message=None)
    assert p.progress == 0.5
    await store.succeed(task_id=t.id, worker_id="w1", result=None)
    done = await pg_client.get(t.id)
    assert done.status == "succeeded" and done.result is None


async def test_lease_recovery_db_clock(pg_client):
    store = pg_client.store
    t = await pg_client.submit("job", {}, max_attempts=3)
    await store.claim(queues=["default"], worker_id="w1", lease_ms=150)
    await asyncio.sleep(0.3)
    c2 = await store.claim(queues=["default"], worker_id="w2", lease_ms=5000)
    assert len(c2) == 1 and c2[0].id == t.id and c2[0].attempt == 2


async def test_concurrent_claims_skip_locked(pg_client):
    # asyncpg + a real pool means these claims genuinely race, so this actually
    # exercises FOR UPDATE SKIP LOCKED (the SQLite version can't — single writer).
    store = pg_client.store
    n = 12
    for i in range(n):
        await pg_client.submit("job", {"i": i})
    batches = await asyncio.gather(
        *(
            store.claim(queues=["default"], worker_id=f"w{k}", lease_ms=5000, limit=1)
            for k in range(n + 6)
        )
    )
    ids = [t.id for batch in batches for t in batch]
    assert len(ids) == n and len(set(ids)) == n


async def test_by_key_transactions(pg_client):
    await pg_client.submit("job", {}, key="K", max_attempts=1)
    canceled = await pg_client.cancel_by_key("K")
    assert canceled.status == "canceled"
    retried = await pg_client.retry_by_key("K", reset_attempt=True)
    assert retried.status == "queued" and retried.attempt == 0
    assert await pg_client.cancel_by_key("missing") is None


async def test_ownership_rejects_non_owner(pg_client):
    store = pg_client.store
    t = await pg_client.submit("job", {})
    await store.claim(queues=["default"], worker_id="owner", lease_ms=5000)
    with pytest.raises(LostLease):
        await store.complete(task_id=t.id, worker_id="intruder", result={})


async def test_worker_end_to_end(pg_client):
    worker = Worker.postgres(DSN, queues=["default"], poll_interval_ms=50)

    @worker.task("sum")
    async def handle(ctx, payload):
        return {"sum": payload["a"] + payload["b"]}

    async with worker.background():
        result = await pg_client.call("sum", {"a": 2, "b": 3}, wait_timeout_ms=10_000, poll_ms=50)
    assert result == {"sum": 5}
