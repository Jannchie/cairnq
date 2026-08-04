"""Live Postgres smoke. Skipped unless CAIRNQ_TEST_PG_DSN is set — CI provides a
`postgres` service; locally point it at any throwaway database. Runs the paths
static review can't prove out: real asyncpg type inference on NULL/jsonb params,
FOR UPDATE SKIP LOCKED under genuine concurrency, the JSON round-trip, DB-clock
lease recovery, and the *_by_key transactions."""

import asyncio
import os
from time import perf_counter

import pytest

from cairnq import CairnQ, PostgresStore, Worker
from cairnq.errors import LostLease

from .helpers import all_terminal, wait_for

DSN = os.environ.get("CAIRNQ_TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set CAIRNQ_TEST_PG_DSN to run live PG tests")


# Unlike the node twin (which isolates itself in its own database), this suite
# shares the DSN database: pytest runs tests sequentially, so nothing races the
# fixture's truncate. Adopting a parallel runner (pytest-xdist) would need the
# same per-suite isolation the node file has.
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


async def test_concurrent_same_key_submits_stay_idempotent(pg_client):
    # Regression: without lock_key.sql these race through READ COMMITTED (no key
    # row to lock yet), every submit sees "no existing task", and one key ends up
    # with several live tasks. The pool gives each submit its own connection, so
    # they truly race.
    results = await asyncio.gather(
        *(pg_client.submit("job", {}, key="K", conflict="reuse") for _ in range(8))
    )
    assert len({t.id for t in results}) == 1
    live = await pg_client.list(name="job", status="queued")
    assert len(live) == 1


async def test_ownership_rejects_non_owner(pg_client):
    store = pg_client.store
    t = await pg_client.submit("job", {})
    await store.claim(queues=["default"], worker_id="owner", lease_ms=5000)
    with pytest.raises(LostLease):
        await store.complete(task_id=t.id, worker_id="intruder", result={})


async def test_notify_wakes_worker_and_waiter_beating_the_poll_floor(pg_client):
    # Poll intervals are set far above the assertion, so finishing in time is
    # only possible if LISTEN/NOTIFY cut both sleeps short: the worker's idle
    # (claim poll 5s) and call's wait poll (4s).
    store = PostgresStore(DSN)
    await store.connect()
    await asyncio.sleep(0.5)  # let the LISTEN connections warm up
    worker = Worker(store, ["default"], poll_interval_ms=5_000)

    @worker.task("ping")
    async def ping(ctx, payload):
        return {"pong": True}

    try:
        t0 = perf_counter()
        async with worker.background():
            await asyncio.sleep(0.3)  # park the worker in its idle sleep first
            result = await pg_client.call("ping", {}, wait_timeout_ms=8_000, poll_ms=4_000)
        assert result == {"pong": True}
        assert perf_counter() - t0 < 3.0
    finally:
        await store.close()


async def test_worker_end_to_end(pg_client):
    worker = Worker.postgres(DSN, queues=["default"], poll_interval_ms=50)

    @worker.task("sum")
    async def handle(ctx, payload):
        return {"sum": payload["a"] + payload["b"]}

    async with worker.background():
        result = await pg_client.call("sum", {"a": 2, "b": 3}, wait_timeout_ms=10_000, poll_ms=50)
    assert result == {"sum": 5}


async def test_batch_delivery_end_to_end(pg_client):
    """Exercises heartbeat_batch.sql's Postgres form — `= any(:ids::text[])` with
    a real asyncpg array bind, which no SQLite run can prove out — and the
    settle-the-rest contract over the DB clock rather than a supplied now_ms."""
    worker = Worker.postgres(
        DSN, queues=["default"], poll_interval_ms=50, concurrency=8, lease_ms=400,
        heartbeat_interval_ms=60, retry_backoff_ms=0,
    )
    seen: list[int] = []

    @worker.task("embed", batch=8)
    async def embed(items):
        seen.append(len(items))
        # Outlive the lease, so the batch heartbeat is what keeps these claimed.
        await asyncio.sleep(0.7)
        for item in items:
            if item.payload["n"] == 1:
                await item.fail("odd one out", retryable=False)
        return {item.task_id: {"n": item.payload["n"]} for item in items}

    ids = [(await pg_client.submit("embed", {"n": n}, max_attempts=1)).id for n in range(4)]
    async with worker.background():
        await wait_for(lambda: all_terminal(pg_client, ids), timeout_s=10.0)

    tasks = {t.payload["n"]: t for t in [await pg_client.get(i) for i in ids]}
    assert seen == [4]
    assert tasks[1].status == "failed" and tasks[1].error["message"] == "odd one out"
    assert [tasks[n].status for n in (0, 2, 3)] == ["succeeded"] * 3
    # Never redelivered despite outliving the lease: one attempt each.
    assert all(t.attempt == 1 for t in tasks.values())
