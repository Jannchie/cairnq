"""Resources: a call ceiling several names draw from.

`concurrency` caps one name against itself, which cannot express the constraint
that actually binds a worker doing heavy local work — several *different*
handlers contending for one scarce thing (a GPU, an index with a single writer).
A resource is that same ceiling with more than one name drawing on it; at
capacity 1 it is mutual exclusion across those names.

The gate is at claim, not inside the handler: a semaphore around the body would
let the task be claimed first, so it would hold a lease, burn a concurrency slot
and heartbeat while waiting its turn.
"""

from __future__ import annotations

import asyncio

import pytest

from cairnq import Worker

from .helpers import all_terminal, wait_for


async def _drain(client, ids, worker):
    async with worker.background():
        await wait_for(lambda: all_terminal(client, ids), timeout_s=5.0)
    return {i: await client.get(i) for i in ids}


async def test_one_resource_excludes_calls_across_names(client, db_path):
    """The headline: two names, capacity 1, never in flight together — even with
    the worker itself free to run four calls."""
    worker = Worker.sqlite(
        db_path, poll_interval_ms=20, concurrency=4, resources={"gpu": 1}
    )
    live = 0
    peak = 0
    seen: set[str] = set()

    async def hold(name: str) -> None:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        seen.add(name)
        await asyncio.sleep(0.03)
        live -= 1

    @worker.task("render", resource="gpu")
    async def render(ctx, payload):
        await hold("render")

    @worker.task("compare", resource="gpu")
    async def compare(ctx, payload):
        await hold("compare")

    ids = [(await client.submit("render", {})).id for _ in range(4)]
    ids += [(await client.submit("compare", {})).id for _ in range(4)]
    tasks = await _drain(client, ids, worker)

    assert peak == 1, f"gpu capacity is 1 but {peak} calls held it at once"
    assert seen == {"render", "compare"}  # both names really did run
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_capacity_above_one_is_a_shared_budget(client, db_path):
    """Not a boolean mutex: capacity 2 lets two of the sharing names run, and
    still no third."""
    worker = Worker.sqlite(
        db_path, poll_interval_ms=20, concurrency=6, resources={"gpu": 2}
    )
    live = 0
    peak = 0

    async def hold(ctx, payload):
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.03)
        live -= 1

    for name in ("render", "compare", "segment"):
        worker.register(name, hold, resource="gpu")

    ids = [
        (await client.submit(name, {})).id
        for name in ("render", "compare", "segment")
        for _ in range(3)
    ]
    tasks = await _drain(client, ids, worker)

    assert peak == 2, f"gpu capacity is 2 but the peak was {peak}"
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_a_resource_does_not_bound_names_outside_it(client, db_path):
    """A resource constrains its members and nothing else — work that does not
    name it keeps running while the resource is saturated."""
    worker = Worker.sqlite(
        db_path, poll_interval_ms=20, concurrency=4, resources={"gpu": 1}
    )
    free_live = 0
    free_peak = 0

    @worker.task("render", resource="gpu")
    async def render(ctx, payload):
        await asyncio.sleep(0.05)

    @worker.task("thumbnail")
    async def thumbnail(ctx, payload):
        nonlocal free_live, free_peak
        free_live += 1
        free_peak = max(free_peak, free_live)
        await asyncio.sleep(0.03)
        free_live -= 1

    ids = [(await client.submit("render", {})).id for _ in range(2)]
    ids += [(await client.submit("thumbnail", {})).id for _ in range(6)]
    tasks = await _drain(client, ids, worker)

    assert free_peak > 1, "an unrelated name was held back by someone else's resource"
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_two_sources_cannot_overshoot_within_one_poll(client, db_path):
    """Regression: the in-flight count only moves when a call is dispatched,
    which happens after the whole plan returns. Without the plan's own tally,
    two names sharing a resource each see its full ceiling in the same poll and
    together exceed it. Both names have work queued before the worker starts, so
    one poll draws for both."""
    worker = Worker.sqlite(
        db_path, poll_interval_ms=20, concurrency=8, resources={"gpu": 1}
    )
    live = 0
    peak = 0

    async def hold(ctx, payload):
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.04)
        live -= 1

    worker.register("render", hold, resource="gpu")
    worker.register("compare", hold, resource="gpu")

    ids = [(await client.submit("render", {})).id for _ in range(3)]
    ids += [(await client.submit("compare", {})).id for _ in range(3)]
    tasks = await _drain(client, ids, worker)

    assert peak == 1, f"one poll drew {peak} calls against a capacity of 1"
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_a_resource_composes_with_batching(client, db_path):
    """Members may batch differently — which is why a resource is a ceiling
    several sources draw down, not one source spanning their names: a source
    carries a single batch size and could not hold both."""
    worker = Worker.sqlite(
        db_path, poll_interval_ms=20, concurrency=4, resources={"gpu": 1}
    )
    live = 0
    peak = 0
    widest = 0

    @worker.task("embed", batch=4, resource="gpu")
    async def embed(items):
        nonlocal live, peak, widest
        live += 1
        peak = max(peak, live)
        widest = max(widest, len(items))
        await asyncio.sleep(0.03)
        live -= 1

    @worker.task("render", resource="gpu")
    async def render(ctx, payload):
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await asyncio.sleep(0.03)
        live -= 1

    ids = [(await client.submit("embed", {})).id for _ in range(8)]
    ids += [(await client.submit("render", {})).id for _ in range(2)]
    tasks = await _drain(client, ids, worker)

    assert peak == 1, f"gpu capacity is 1 but {peak} calls held it at once"
    assert widest == 4, "the batched member did not fill its batch"
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_the_tighter_of_name_and_resource_binds(client, db_path):
    """A name's own concurrency and its resource are independent ceilings; the
    smaller one wins, and neither is relaxed by the other."""
    worker = Worker.sqlite(
        db_path, poll_interval_ms=20, concurrency=8, resources={"gpu": 3}
    )
    render_live = 0
    render_peak = 0
    total_live = 0
    total_peak = 0

    async def hold(counter):
        nonlocal total_live, total_peak
        total_live += 1
        total_peak = max(total_peak, total_live)
        await asyncio.sleep(0.03)
        total_live -= 1

    @worker.task("render", concurrency=1, resource="gpu")
    async def render(ctx, payload):
        nonlocal render_live, render_peak, total_live, total_peak
        render_live += 1
        render_peak = max(render_peak, render_live)
        total_live += 1
        total_peak = max(total_peak, total_live)
        await asyncio.sleep(0.03)
        total_live -= 1
        render_live -= 1

    @worker.task("compare", resource="gpu")
    async def compare(ctx, payload):
        await hold(None)

    ids = [(await client.submit("render", {})).id for _ in range(4)]
    ids += [(await client.submit("compare", {})).id for _ in range(6)]
    tasks = await _drain(client, ids, worker)

    assert render_peak == 1, f"render caps itself at 1 but ran {render_peak}"
    assert total_peak <= 3, f"gpu capacity is 3 but the peak was {total_peak}"
    assert all(t.status == "succeeded" for t in tasks.values())


async def test_the_units_come_back_when_a_call_fails(client, db_path):
    """A resource is refunded on every way out of a call, not just the happy
    one — otherwise a failing name leaks its capacity and wedges the rest."""
    worker = Worker.sqlite(
        db_path,
        poll_interval_ms=20,
        concurrency=4,
        retry_backoff_ms=0,
        resources={"gpu": 1},
    )
    ran = 0

    @worker.task("boom", resource="gpu")
    async def boom(ctx, payload):
        raise RuntimeError("nope")

    @worker.task("after", resource="gpu")
    async def after(ctx, payload):
        nonlocal ran
        ran += 1

    ids = [(await client.submit("boom", {}, max_attempts=1)).id for _ in range(3)]
    ids += [(await client.submit("after", {})).id for _ in range(3)]
    tasks = await _drain(client, ids, worker)

    assert ran == 3, "the failing name leaked its resource units"
    assert all(t.status == "failed" for i, t in tasks.items() if t.name == "boom")
    assert all(t.status == "succeeded" for i, t in tasks.items() if t.name == "after")


async def test_an_undeclared_resource_is_rejected_at_registration(db_path):
    """A typo would otherwise read as an unbounded resource — silently removing
    the ceiling the caller asked for, which is the whole point of the option."""
    worker = Worker.sqlite(db_path, resources={"gpu": 1})

    with pytest.raises(ValueError, match="gpu"):
        worker.register("render", lambda ctx, payload: None, resource="gpü")


async def test_a_resource_capacity_below_one_is_rejected(db_path):
    with pytest.raises(ValueError, match="must be >= 1"):
        Worker.sqlite(db_path, resources={"gpu": 0})
