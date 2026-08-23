"""One side of a differential run: performs a scenario's workload against `db`
using the Python SDK, then exits. Mirrors node_driver.ts step for step — the
comparison blames the SDK for any difference, so a difference in the driver
would be blamed on the wrong thing.

Every submit stamps ``metadata.dt_key``: identity in the dump comes from the
workload, not from the random ULIDs. See dump.mjs."""

import asyncio
import sys

from cairnq import CairnQ, Retention, RetentionSweeper, Worker


async def close_drains_queue(db: str) -> None:
    """Writes accepted but not awaited, then an immediate close.

    SQLiteStore batches writes already waiting on its lock into one transaction,
    so at the moment close() is called a group commit is holding submits whose
    callers are still awaiting them. A close that does not drain loses exactly
    those rows — and the count that comes back is the only evidence."""
    tasks = CairnQ.sqlite(db)
    await tasks.connect()
    in_flight = [
        asyncio.create_task(tasks.submit("job", {"i": i}, metadata={"dt_key": f"t{i:02d}"}))
        for i in range(64)
    ]
    await asyncio.sleep(0)  # let the submits reach the store, as JS scheduling does
    await tasks.close()
    await asyncio.gather(*in_flight, return_exceptions=True)


async def background_failure_is_reported(db: str) -> None:
    """A worker that cannot start, and whether the SDK says so.

    The outcome is written INTO the database as a task, because that is the only
    channel the two sides' observations can be compared through. Node used to let
    run()'s rejection go unattached and its default handling killed the process,
    so `outcome` was never submitted at all — a MISSING_TASK. Python suppressed
    the failure, so it recorded reported=False. Both now record reported=True."""
    tasks = CairnQ.sqlite(db)
    await tasks.connect()
    await tasks.submit("marker", {"at": "start"}, metadata={"dt_key": "t00"})

    # A path no process can open, so connect() fails inside run().
    worker = Worker.sqlite("/proc/cairnq-cannot-exist/tasks.db")

    @worker.task("noop")
    async def _noop(ctx, payload):
        return None

    reported = False
    body_ran = False
    try:
        async with worker.background():
            await asyncio.sleep(0.05)
            body_ran = True
    except Exception:
        reported = True

    await tasks.submit(
        "outcome", {"reported": reported, "bodyRan": body_ran}, metadata={"dt_key": "t01"}
    )
    await tasks.close()


async def sweeper_stop_start(db: str) -> None:
    """A sweeper stopped and started again, and one drained by hand afterwards.

    `_stop` is how a sweep in flight cuts itself short, and stop() used to leave
    it set: a later sweep() returned after its FIRST batch, and a restarted
    sweeper never swept at all. Both show up as rows that should be gone."""
    tasks = CairnQ.sqlite(db)
    await tasks.connect()
    for i in range(7):
        t = await tasks.submit("job", {"i": i}, metadata={"dt_key": f"t{i:02d}"})
        await tasks.store.claim(queues=["default"], worker_id="w1", lease_ms=5_000)
        await tasks.store.succeed(task_id=t.id, worker_id="w1", result={"i": i})
    # A survivor, so "swept everything" and "swept nothing" are distinguishable.
    await tasks.submit("keep", {}, metadata={"dt_key": "keep"})

    sweeper = RetentionSweeper(tasks.store, Retention(older_than_ms=0, limit=2))
    sweeper.start()
    await sweeper.stop()
    await sweeper.sweep()  # the on-demand drain a stopped sweeper used to truncate
    await tasks.close()


async def unserializable_result(db: str) -> None:
    """A handler returning a value its language cannot put in JSON.

    Node used to write `Map` as `{}` — succeeded, with the result silently gone.
    Python's encoder always raised. Both must now record the same permanent
    `unserializable_result` failure. The offending VALUE is constructed in each
    language, which is precisely why this cannot be a conformance scenario: JSON
    has no way to write a set."""
    tasks = CairnQ.sqlite(db)
    await tasks.connect()
    worker = Worker.sqlite(db, poll_interval_ms=20, retry_backoff_ms=0)

    @worker.task("opaque")
    async def _opaque(ctx, payload):
        return {1, 2}

    @worker.task("plain")
    async def _plain(ctx, payload):
        return {"echoed": payload["i"]}

    await tasks.submit("opaque", {}, metadata={"dt_key": "t00"}, max_attempts=1)
    await tasks.submit("plain", {"i": 1}, metadata={"dt_key": "t01"})

    async with worker.background():
        for _ in range(200):
            all_ = await tasks.list()
            if len(all_) == 2 and all(t.is_terminal for t in all_):
                break
            await asyncio.sleep(0.025)
    await tasks.close()


SCENARIOS = {
    "close_drains_queue": close_drains_queue,
    "background_failure_is_reported": background_failure_is_reported,
    "sweeper_stop_start": sweeper_stop_start,
    "unserializable_result": unserializable_result,
}


async def main() -> None:
    db, scenario = sys.argv[1], sys.argv[2]
    run = SCENARIOS.get(scenario)
    if run is None:
        print(f"unknown scenario: {scenario}", file=sys.stderr)
        raise SystemExit(2)
    await run(db)
    print("DRIVER_DONE", flush=True)


asyncio.run(main())
