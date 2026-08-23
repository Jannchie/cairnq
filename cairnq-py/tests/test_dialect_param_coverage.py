"""Every operation must supply what BOTH dialects' statements bind.

Since the refactor each operation builds one dialect-neutral superset of
parameters and each dialect takes what its own SQL asks for. That makes a missing
name invisible on SQLite whenever only Postgres binds it — and the Postgres path
can't run without a live database, so nothing else here would catch it.

This drives every operation through the real SQLite store while recording the
superset it passed, then checks that superset against what each dialect's
statement actually binds."""

from __future__ import annotations

import pytest

from cairnq._sql import load_statements
from cairnq.store.base import statement_params
from cairnq.store.sqlite import SQLiteStore

DIALECTS = {name: load_statements(name) for name in ("sqlite", "postgres")}


class RecordingStore(SQLiteStore):
    """A real store that also remembers the parameter names each op passed."""

    def __init__(self, path: str):
        super().__init__(path)
        self.seen: dict[str, set[str]] = {}

    async def _run(self, name, params):  # type: ignore[override]
        self.seen.setdefault(name, set()).update(params)
        return await super()._run(name, params)


async def _exercise_every_operation(store: RecordingStore) -> None:
    """Touch each protocol statement at least once."""
    await store.connect()
    worker = "w1"

    # Client side, including every key-conflict branch.
    task = await store.submit(name="job", payload={"v": 1}, delay_ms=0)
    await store.submit(name="job", payload={}, key="k", conflict="reuse")
    await store.submit(name="job", payload={}, key="k", conflict="reuse")
    await store.submit(name="job", payload={}, key="k", conflict="reuse-succeeded")
    await store.submit(name="job", payload={}, key="k", conflict="replace")
    await store.get(task.id)
    await store.get_by_key("k")
    await store.get_status(task.id)
    await store.get_status_by_key("k")
    await store.list(status="queued", queue="default", name="job")
    await store.cancel_by_key("k")
    await store.retry_by_key("k", reset_attempt=True)

    # Worker side. One claim of several tasks, so each terminal write has its own
    # task to act on — a fail() would otherwise push the only task into the future
    # and leave succeed/complete with nothing to run against.
    for i in range(3):
        await store.submit(name="job", payload={"i": i})
    claimed = await store.claim(queues=["default"], worker_id=worker, lease_ms=5_000, limit=10)
    assert len(claimed) >= 3, f"need three running tasks, claimed {len(claimed)}"

    await store.heartbeat(task_id=claimed[0].id, worker_id=worker, lease_ms=5_000)
    # The batch-delivery heartbeat, while every claimed task is still running —
    # it renews only rows this worker holds a live lease on.
    await store.heartbeat_batch(
        task_ids=[t.id for t in claimed], worker_id=worker, lease_ms=5_000
    )
    await store.progress(task_id=claimed[0].id, worker_id=worker, progress=0.5, message="x")
    await store.fail(
        task_id=claimed[0].id, worker_id=worker, error={"m": 1}, retryable=True, delay_ms=10
    )
    await store.succeed(task_id=claimed[1].id, worker_id=worker, result={"ok": True})
    await store.complete(task_id=claimed[2].id, worker_id=worker, result={"ok": True})

    # All four claim statements. Each list-valued filter has an equality variant,
    # picked per draw, and the four combinations bind different parameters — so
    # exercising only one leaves the others' supersets unchecked, which is exactly
    # what this file is for. The claim above watched one queue and passed no
    # names, so it took claim_one_queue; the rest are covered here.
    # One fresh task before each, or the probe short-circuits an empty claim and
    # the statement never runs — which this test would then read as uncovered.
    await store.submit(name="job", payload={}, queue="other")
    await store.claim(queues=["default", "other"], worker_id=worker, lease_ms=5_000, limit=1)
    await store.submit(name="job", payload={})
    await store.claim(
        queues=["default"], names=["job"], worker_id=worker, lease_ms=5_000, limit=1
    )
    await store.submit(name="job", payload={})
    await store.claim(
        queues=["default", "other"], names=["job"], worker_id=worker, lease_ms=5_000, limit=1
    )

    await store.cancel(task.id)
    await store.retry(task.id, reset_attempt=False)
    await store.queue_depth("default", 10)
    # Every filter combination: each picks a different statement, so calling one
    # shape would leave the other three unbound and unchecked.
    await store.purge(older_than_ms=0, limit=10)
    await store.purge(older_than_ms=0, status="succeeded", name="job", limit=10)
    await store.purge(older_than_ms=0, queue="default", limit=10)
    await store.purge(older_than_ms=0, queue="default", status="succeeded", limit=10)


@pytest.fixture
async def recorded(db_path):
    store = RecordingStore(db_path)
    try:
        await _exercise_every_operation(store)
        yield store
    finally:
        await store.close()


async def test_every_statement_is_exercised(recorded):
    """Guards the test itself: a statement nobody calls proves nothing below."""
    untouched = set(DIALECTS["sqlite"]) - set(recorded.seen)
    assert not untouched, f"statements never exercised, so unchecked: {sorted(untouched)}"


@pytest.mark.parametrize("dialect", sorted(DIALECTS))
async def test_operations_supply_every_parameter_each_dialect_binds(recorded, dialect):
    missing: dict[str, set[str]] = {}
    for name, supplied in recorded.seen.items():
        sql = DIALECTS[dialect].get(name)
        if sql is None:
            continue  # e.g. claimable_probe exists only for sqlite
        # SQLite derives some names from the neutral ones it was handed, so credit
        # both the superset and what its binder produces from it.
        available = supplied | _derived(supplied)
        required = set(statement_params(sql))
        if required - available:
            missing[name] = required - available
    assert not missing, f"{dialect} statements missing parameters: {missing}"


def _derived(supplied: set[str]) -> set[str]:
    """Names a dialect binder computes from the neutral ones (see SQLiteStore._bind)."""
    out = {"now_ms"}
    if "lease_ms" in supplied:
        out.add("lease_until_ms")
    if "delay_ms" in supplied:
        out.add("run_at_ms")
    if "older_than_ms" in supplied:
        out.add("before_ms")
    return out
