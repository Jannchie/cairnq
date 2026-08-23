"""One side of a differential run: performs a scenario's workload against `db`
using the Python SDK, then exits. Mirrors node_driver.ts step for step — the
comparison blames the SDK for any difference, so a difference in the driver
would be blamed on the wrong thing.

Every submit stamps ``metadata.dt_key``: identity in the dump comes from the
workload, not from the random ULIDs. See dump.mjs."""

import asyncio
import sys

from cairnq import CairnQ, Worker

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
    "background_failure_is_reported": background_failure_is_reported,
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
