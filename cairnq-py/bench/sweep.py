"""Parameter sweeps (Python SDK).

    uv run python bench/sweep.py               # SQLite on a temp file
    uv run python bench/sweep.py postgres      # against CAIRNQ_TEST_PG_DSN (throwaway DB)

run.py answers "how fast is CairnQ". This answers "where does the time go, and
which knob moves it" — so it reports breakdowns and ratios, not headline
throughput. Workload sizes and row labels MUST match cairnq-node/bench/sweep.ts.

    A  drain breakdown        where a drain's wall time goes, at concurrency 1..64
    B  finalize batching      what a batched finalize COULD buy, without an API
    C  claim batch size       what claim(limit=N) already buys
    D  claim vs queue count   the cost of watching more than one queue

B is the point of the file: it measures the ceiling on a batch-finalize API by
running today's `complete` statements inside one transaction, so the API only
gets designed if the ceiling is worth having.

Every number is a median of REPEATS runs on a database freshly created for that
data point. Both matter: a single run of these varies by ~2x, and a database
reused across data points carries the churn of the earlier ones into the
planner's decisions — D is where that showed up as a 3x inflation.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from statistics import median
from time import perf_counter
from typing import Any, Awaitable, Callable

from cairnq import CairnQ, PostgresStore, SQLiteStore, TaskDef, TaskStore, Worker

REPEATS = 3
CONCURRENCIES = [1, 8, 32, 64]
N_DRAIN = 400
N_FINALIZE = 200
N_BATCH = 64
CLAIM_LIMITS = [1, 8, 64]
BACKLOGS = [500, 5_000, 20_000]
N_CLAIM_SAMPLES = 25
QUEUE_SETS = [["default"], ["default", "q2"], ["default", "q2", "q3"]]

noop = TaskDef("bench.noop")
WORKER_ID = "bench-worker"


def ms(v: float) -> str:
    return f"{v:.1f}ms"


def us(v: float) -> str:
    return f"{v * 1000:.0f}µs"


@dataclass
class Hit:
    """Per-statement timings, so a phase breakdown needs no guesswork about which
    protocol statement a phase maps to. A transaction may be replayed under
    contention; a retried statement is counted again, which is why `n` is reported
    next to the totals rather than assumed equal to the workload."""

    n: int = 0
    ms: float = 0.0


def instrument(store: TaskStore) -> TaskStore:
    """Time every protocol statement, both inside and outside transactions.

    The seam is private, so this rebinds it on the instance — base.py calls
    `self._fetch` / `self._transaction`, and an instance attribute shadows the
    method for both paths at once."""
    hits: dict[str, Hit] = {}
    orig_fetch = store._fetch
    orig_tx = store._transaction

    async def timed(name: str, run: Callable[[], Awaitable[list[Any]]]) -> list[Any]:
        t0 = perf_counter()
        try:
            return await run()
        finally:
            h = hits.setdefault(name, Hit())
            h.n += 1
            h.ms += (perf_counter() - t0) * 1000

    async def fetch(name: str, params: dict[str, Any]) -> list[Any]:
        return await timed(name, lambda: orig_fetch(name, params))

    @asynccontextmanager
    async def transaction():
        async with orig_tx() as inner:

            async def wrapped(name: str, params: dict[str, Any]) -> list[Any]:
                return await timed(name, lambda: inner(name, params))

            yield wrapped

    store._fetch = fetch  # type: ignore[method-assign]
    store._transaction = transaction  # type: ignore[method-assign]
    store.hits = hits  # type: ignore[attr-defined]
    store.reset = hits.clear  # type: ignore[attr-defined]
    return store


@dataclass
class Bed:
    """One client + one instrumented store on a database nothing else has touched.

    `analyze` is the backend's own out-of-band ANALYZE. Both stores refresh
    statistics on their own, but on a minute's interval — a backlog built in
    seconds would otherwise be planned against the empty table it replaced, which
    is a different measurement than the one D is after.

    `seed` bulk-inserts queued rows. Where a backlog is scenery rather than the
    thing being measured (D), building it through submit() costs one round trip per
    row — minutes on a networked Postgres, and none of it timed."""

    tasks: CairnQ
    store: TaskStore
    analyze: Callable[[], Awaitable[None]]
    seed: Callable[[int, str], Awaitable[None]]

    @property
    def hits(self) -> dict[str, Hit]:
        return self.store.hits  # type: ignore[attr-defined]


@asynccontextmanager
async def sqlite_bed():
    with tempfile.TemporaryDirectory(prefix="cairnq-sweep-") as tmp:
        path = os.path.join(tmp, "tasks.db")
        tasks = CairnQ.sqlite(path)
        store = instrument(SQLiteStore(path))
        await tasks.connect()
        await store.connect()

        async def analyze() -> None:
            # A separate connection, so this cannot be mistaken for something the
            # store does on its own schedule.
            db = sqlite3.connect(path)
            try:
                db.execute("ANALYZE cairnq_tasks")
                db.commit()
            finally:
                db.close()

        async def seed(n: int, queue: str) -> None:
            db = sqlite3.connect(path)
            try:
                db.executemany(
                    "insert into cairnq_tasks (id, name, queue, status, payload, run_at_ms,"
                    " created_at_ms, updated_at_ms) values (?, ?, ?, 'queued', '{}', 0, ?, ?)",
                    [(f"seed-{queue}-{i}", noop.name, queue, i, i) for i in range(n)],
                )
                db.commit()
            finally:
                db.close()

        # Close inside the tempdir's scope, not after it: aiosqlite runs each
        # connection on its own thread, and leaving one open against a deleted
        # file leaves that thread blocked forever.
        try:
            yield Bed(tasks, store, analyze, seed)
        finally:
            await tasks.close()
            await store.close()


@asynccontextmanager
async def postgres_bed(dsn: str):
    import asyncpg

    tasks = CairnQ.postgres(dsn)
    store = instrument(PostgresStore(dsn))
    await tasks.connect()
    await store.connect()
    conn = await asyncpg.connect(dsn)
    # One shared database, so "fresh" means emptied and re-analyzed rather than
    # recreated — a stale row estimate would otherwise leak into the next point.
    # Delete outright rather than purge: purge only sweeps terminal rows, and a
    # queued or running row left by the previous data point would be claimed by the
    # next one's worker and counted in its numbers.
    await conn.execute("delete from cairnq_tasks")

    async def analyze() -> None:
        await conn.execute("analyze cairnq_tasks")

    async def seed(n: int, queue: str) -> None:
        await conn.execute(
            """insert into cairnq_tasks (id, name, queue, status, payload, run_at_ms,
                 created_at_ms, updated_at_ms)
               select 'seed-' || $1 || '-' || i, $2, $1, 'queued', '{}'::jsonb, 0, i, i
               from generate_series(1, $3) as i""",
            queue,
            noop.name,
            n,
        )

    await analyze()
    try:
        yield Bed(tasks, store, analyze, seed)
    finally:
        await conn.execute("delete from cairnq_tasks")
        await conn.close()
        await tasks.close()
        await store.close()


BedFactory = Callable[[], Any]


async def repeat(bed_of: BedFactory, fn: Callable[[Bed], Awaitable[float]]) -> float:
    """Run `fn` on a fresh bed, REPEATS times, and return the median result."""
    out = []
    for _ in range(REPEATS):
        async with bed_of() as bed:
            out.append(await fn(bed))
    return median(out)


async def submit_many(tasks: CairnQ, n: int, queue: str = "default") -> None:
    await asyncio.gather(*(tasks.submit(noop, {"i": i}, queue=queue) for i in range(n)))


async def finalize_all(bed: Bed) -> None:
    for t in await bed.tasks.list(status="running", limit=10_000):
        await bed.store.complete(task_id=t.id, worker_id=WORKER_ID, result={})


async def claim_all(bed: Bed, n: int, limit: int | None = None) -> list[str]:
    """Claim exactly `n` tasks, however many round trips that takes."""
    out: list[str] = []
    while len(out) < n:
        got = await bed.store.claim(
            queues=["default"],
            worker_id=WORKER_ID,
            lease_ms=600_000,
            limit=min(limit or n, n - len(out)),
            names=[noop.name],
        )
        if not got:
            raise RuntimeError("nothing claimable")
        out.extend(t.id for t in got)
    return out


# --------------------------------------------------------------------- sweep A
# A no-op handler makes the handler term ~0, so whatever wall time remains is the
# runtime's own: claim, finalize, and the idle gaps between polls.
#
# Every statement gets a row rather than a hand-picked few: the worker finalizes
# with `complete`, not `succeed` (cancel-vs-success has to be decided in the same
# write), and a breakdown that assumes otherwise silently reports a zero.
#
# The share-of-wall column is printed only at concurrency 1. Above it the timings
# overlap — a statement's duration includes waiting for the store's in-process
# lock — so the shares would sum past 100% and mean nothing. Read the per-call
# column there instead: it rising with concurrency IS the queueing. Contention-
# free per-op costs come from B and C, which run no worker at all.
async def drain_once(bed: Bed, conc: int) -> float:
    await submit_many(bed.tasks, N_DRAIN)
    done = 0
    all_done = asyncio.Event()

    def counting_noop(ctx, payload):
        nonlocal done
        done += 1
        if done == N_DRAIN:
            all_done.set()
        return {}

    w = Worker(bed.store, ["default"], concurrency=conc)
    w.task(noop)(counting_noop)
    bed.store.reset()  # type: ignore[attr-defined]
    t0 = perf_counter()
    async with w.background():
        await all_done.wait()
        while await bed.tasks.list(status="running", limit=1):
            await asyncio.sleep(0.005)
    return (perf_counter() - t0) * 1000


async def drain_breakdown(bed_of: BedFactory) -> None:
    print(f"\nA  drain breakdown — {N_DRAIN} no-op tasks, median of {REPEATS}")
    for conc in CONCURRENCIES:
        # The breakdown comes from the last repeat; the wall time is the median, so
        # a slow outlier cannot dominate the headline number it sits next to.
        seen: dict[str, Hit] = {}

        async def one(bed: Bed, conc: int = conc) -> float:
            nonlocal seen
            wall = await drain_once(bed, conc)
            seen = dict(bed.hits)
            return wall

        wall = await repeat(bed_of, one)
        serial = conc == 1
        in_sql = sum(h.ms for h in seen.values())
        share = lambda v: f"{v / wall * 100:.1f}%"  # noqa: E731
        tail = f"{share(in_sql)} inside protocol statements" if serial else ""
        print(f"   conc {conc:<3}{ms(wall):<10}{f'{round(N_DRAIN / wall * 1000)} tasks/s':<14}{tail}")
        for name, h in sorted(seen.items(), key=lambda kv: -kv[1].ms):
            tail = f"{ms(h.ms)} total  {share(h.ms)} of wall" if serial else ""
            print(f"     {name:<20}{f'n={h.n}':<8}{f'{us(h.ms / h.n)}/call':<12}{tail}")


# --------------------------------------------------------------------- sweep B
# The decisive one. Same statements, same rows, same ownership checks — the only
# difference is how many transactions they are spread over.
async def finalize_batching(bed_of: BedFactory) -> None:
    print(f"\nB  finalize batching — {N_FINALIZE} completes, median of {REPEATS}")

    async def one_txn_each(bed: Bed) -> float:
        await submit_many(bed.tasks, N_FINALIZE)
        ids = await claim_all(bed, N_FINALIZE)
        t0 = perf_counter()
        for task_id in ids:
            await bed.store.complete(task_id=task_id, worker_id=WORKER_ID, result={})
        return (perf_counter() - t0) * 1000

    async def all_in_one(bed: Bed) -> float:
        await submit_many(bed.tasks, N_FINALIZE)
        ids = await claim_all(bed, N_FINALIZE)
        t0 = perf_counter()
        async with bed.store._transaction() as fetch:
            for task_id in ids:
                await fetch("complete", {"id": task_id, "worker_id": WORKER_ID, "result": "{}"})
        return (perf_counter() - t0) * 1000

    serial = await repeat(bed_of, one_txn_each)
    batched = await repeat(bed_of, all_in_one)
    print(f"   {'one txn each':<24}{ms(serial):<10}{us(serial / N_FINALIZE)}/task")
    print(f"   {'all in one txn':<24}{ms(batched):<10}{us(batched / N_FINALIZE)}/task")
    print(f"   {'ceiling on a batch API':<24}{serial / batched:.1f}x, on the finalize step alone")


# --------------------------------------------------------------------- sweep C
# The counterpart to B, at the store level: how claim cost scales with limit.
async def claim_batch_size(bed_of: BedFactory) -> None:
    print(f"\nC  claim batch size — moving {N_BATCH} tasks into 'running', median of {REPEATS}")
    for limit in CLAIM_LIMITS:

        async def one(bed: Bed, limit: int = limit) -> float:
            await submit_many(bed.tasks, N_BATCH)
            t0 = perf_counter()
            await claim_all(bed, N_BATCH, limit)
            dt = (perf_counter() - t0) * 1000
            await finalize_all(bed)
            return dt

        wall = await repeat(bed_of, one)
        print(f"   {f'limit={limit}':<24}{ms(wall):<10}{us(wall / N_BATCH)}/task")


# --------------------------------------------------------------------- sweep D
# Re-derives the numbers PROTOCOL.md quotes, so they can be re-checked after a
# SQLite or driver upgrade rather than trusted indefinitely. The backlog is split
# evenly over the watched queues: a worker watching two queues with work in only
# one is the easy case, and measuring it would understate the cost.
async def claim_vs_queue_count(bed_of: BedFactory) -> None:
    print(f"\nD  claim cost vs queue count — median claim of {N_CLAIM_SAMPLES}, backlog split evenly")
    header = "".join(f"{f'{len(q)} queue' + ('s' if len(q) > 1 else ''):<12}" for q in QUEUE_SETS)
    print(f"   {'backlog':<10}{header}")
    for backlog in BACKLOGS:
        cells = []
        for queues in QUEUE_SETS:

            async def one(bed: Bed, queues: list[str] = queues, backlog: int = backlog) -> float:
                for q in queues:
                    await bed.seed(round(backlog / len(queues)), q)
                await bed.analyze()
                lat = []
                for _ in range(N_CLAIM_SAMPLES):
                    t0 = perf_counter()
                    rows = await bed.store.claim(
                        queues=queues,
                        worker_id=WORKER_ID,
                        lease_ms=600_000,
                        limit=1,
                        names=[noop.name],
                    )
                    lat.append((perf_counter() - t0) * 1000)
                    if not rows:
                        break
                    await bed.store.complete(
                        task_id=rows[0].id, worker_id=WORKER_ID, result={}
                    )
                return median(lat)

            cells.append(f"{us(await repeat(bed_of, one)):<12}")
        print(f"   {backlog:<10}{''.join(cells)}")


async def main() -> None:
    backend = sys.argv[1] if len(sys.argv) > 1 else "sqlite"
    if backend == "postgres":
        dsn = os.environ.get("CAIRNQ_TEST_PG_DSN")
        if not dsn:
            raise SystemExit("set CAIRNQ_TEST_PG_DSN (point it at a throwaway database)")
        bed_of: BedFactory = lambda: postgres_bed(dsn)  # noqa: E731
    else:
        bed_of = sqlite_bed
    print(f"cairnq-py sweep  backend={backend}  repeats={REPEATS}", flush=True)
    await drain_breakdown(bed_of)
    await finalize_batching(bed_of)
    await claim_batch_size(bed_of)
    await claim_vs_queue_count(bed_of)


if __name__ == "__main__":
    asyncio.run(main())
