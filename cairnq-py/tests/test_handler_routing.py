"""A worker must not claim work it cannot run.

`claim` filters by queue, so two workers with different handler sets on one queue
both see every task. A worker that wins a task it has no handler for used to fail
it permanently — which is exactly the mixed-language deployment the README sells
(a Python API next to a TypeScript worker on the default queue), and it destroyed
whichever tasks the wrong worker happened to win the race for."""

from __future__ import annotations

import asyncio

from cairnq import CairnQ, Worker

from .helpers import all_terminal, wait_for


async def test_a_worker_leaves_tasks_it_cannot_run_for_the_worker_that_can(db_path):
    client = CairnQ.sqlite(db_path)
    await client.connect()
    alpha = Worker.sqlite(db_path, poll_interval_ms=10)
    beta = Worker.sqlite(db_path, poll_interval_ms=10)
    alpha.task("alpha")(lambda ctx: {"by": "alpha"})
    beta.task("beta")(lambda ctx: {"by": "beta"})

    try:
        async with alpha.background(), beta.background():
            # Enough tasks that "the right worker won every race" is not a
            # plausible explanation for them all succeeding.
            tasks = [await client.submit("beta", {"i": i}) for i in range(20)]
            await wait_for(
                lambda: all_terminal(client, [t.id for t in tasks]), timeout_s=10.0
            )
            final = [await client.get(t.id) for t in tasks]
    finally:
        await client.close()

    assert [t.status for t in final] == ["succeeded"] * 20, (
        f"tasks lost to the wrong worker: {[(t.status, t.error) for t in final if not t.succeeded]}"
    )
    assert all(t.result == {"by": "beta"} for t in final)


async def test_a_worker_with_no_handlers_claims_nothing(db_path):
    """The degenerate case of the same rule: nothing registered, nothing claimed —
    rather than claiming everything and failing all of it."""
    client = CairnQ.sqlite(db_path)
    await client.connect()
    idle = Worker.sqlite(db_path, poll_interval_ms=10)
    try:
        async with idle.background():
            task = await client.submit("job", {})
            await asyncio.sleep(0.3)
            current = await client.get(task.id)
    finally:
        await client.close()

    assert current.queued, f"an empty worker took the task: {current.status}"
    assert current.attempt == 0
