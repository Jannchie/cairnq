"""CairnQ micro-benchmark (Python SDK).

    uv run python bench/run.py               # SQLite on a temp file
    uv run python bench/run.py postgres      # against CAIRNQ_TEST_PG_DSN (use a throwaway DB)

Workload sizes, poll settings and row labels MUST match cairnq-node/bench/run.ts
— the two benches exist to be compared against each other.

Reports client-op throughput (submit/get/cancel/purge), worker drain throughput,
and end-to-end call() latency — the latency rows are the ones that show the
polling floor: with the default intervals a call can't finish faster than the
worker's claim poll plus wait()'s read poll.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from time import perf_counter

from cairnq import CairnQ, PostgresStore, SQLiteStore, TaskDef, TaskStore, Worker

N_SUBMIT = 1000
N_GET = 2000
N_DRAIN = 500
N_CALL = 40

noop = TaskDef("bench.noop")
rows: list[list[str]] = []


def ops_per_sec(n: int, ms: float) -> int:
    return round(n / ms * 1000)


def pct(sorted_ms: list[float], q: float) -> float:
    return sorted_ms[min(len(sorted_ms) - 1, int(q * len(sorted_ms)))]


async def timed(label, n, fn):
    t0 = perf_counter()
    out = await fn()
    rows.append([label, f"{ops_per_sec(n, (perf_counter() - t0) * 1000)} ops/s"])
    return out


def push_latency(label: str, lat: list[float]) -> None:
    rows.append([label, f"p50 {pct(lat, 0.5):.0f} ms", f"p95 {pct(lat, 0.95):.0f} ms"])


async def call_latencies(tasks: CairnQ, **call_kwargs) -> list[float]:
    lat = []
    for _ in range(N_CALL):
        t0 = perf_counter()
        await tasks.call(noop, {}, timeout_ms=15_000, **call_kwargs)
        lat.append((perf_counter() - t0) * 1000)
    return sorted(lat)


async def purge_all(tasks: CairnQ) -> None:
    """Sweep every terminal task, honoring purge's bounded-batch contract."""
    while len(await tasks.purge(older_than_ms=0, limit=1_000)) == 1_000:
        pass


async def drain_settled(tasks: CairnQ) -> None:
    """The drain counter fires on the last handler's return; its complete write —
    and the handful still in flight — land just after. Wait them out."""
    while await tasks.list(status="running", limit=1):
        await asyncio.sleep(0.005)


async def run(backend: str, tasks: CairnQ, worker_store: TaskStore) -> None:
    await tasks.connect()

    # ---- client ops, no worker running. "bench.unclaimed" is a name no worker
    # registers, so these rows sit queued until the cancel pass below.
    async def submit_all() -> list[str]:
        return [(await tasks.submit("bench.unclaimed", {"i": i})).id for i in range(N_SUBMIT)]

    ids = await timed("submit", N_SUBMIT, submit_all)
    n_ids = len(ids)

    async def get_all() -> None:
        for i in range(N_GET):
            await tasks.get(ids[i % n_ids])

    await timed("get", N_GET, get_all)

    async def cancel_all() -> None:
        for task_id in ids:
            await tasks.cancel(task_id)

    await timed("cancel", N_SUBMIT, cancel_all)
    await timed("purge", N_SUBMIT, lambda: purge_all(tasks))

    # ---- drain throughput + call latency at the default poll intervals. The
    # worker store connects up front so one-time setup stays out of the clock;
    # both workers share it (they never run at the same time).
    await asyncio.gather(*(tasks.submit(noop, {"i": i}) for i in range(N_DRAIN)))
    done = 0
    all_done = asyncio.Event()

    def counting_noop(ctx, payload):
        nonlocal done
        done += 1
        if done == N_DRAIN:
            all_done.set()
        return {}

    w = Worker(worker_store, ["default"], concurrency=8)
    w.task(noop)(counting_noop)
    await worker_store.connect()
    t0 = perf_counter()
    async with w.background():
        await all_done.wait()
        await drain_settled(tasks)
        drain_ms = (perf_counter() - t0) * 1000
        lat = await call_latencies(tasks)
    rows.append([f"drain {N_DRAIN} (conc 8)", f"{ops_per_sec(N_DRAIN, drain_ms)} tasks/s"])
    push_latency("call e2e (default polls)", lat)

    # ---- call latency with the poll intervals tuned down.
    w2 = Worker(worker_store, ["default"], concurrency=8, poll_interval_ms=25)
    w2.task(noop)(lambda ctx, payload: {})
    async with w2.background():
        lat2 = await call_latencies(tasks, poll_ms=10)
    push_latency("call e2e (poll 25ms, wait 10ms)", lat2)

    if backend == "postgres":
        await purge_all(tasks)  # shared DB — leave it clean
    await tasks.close()
    await worker_store.close()

    print(f"cairnq-py bench  backend={backend}")
    for row in rows:
        print(f"  {row[0]:<32}{'  '.join(row[1:])}")


async def main() -> None:
    backend = sys.argv[1] if len(sys.argv) > 1 else "sqlite"
    if backend == "postgres":
        dsn = os.environ.get("CAIRNQ_TEST_PG_DSN")
        if not dsn:
            raise SystemExit("set CAIRNQ_TEST_PG_DSN (point it at a throwaway database)")
        await run(backend, CairnQ.postgres(dsn), PostgresStore(dsn))
    else:
        with tempfile.TemporaryDirectory(prefix="cairnq-") as tmp:
            path = os.path.join(tmp, "tasks.db")
            await run(backend, CairnQ.sqlite(path), SQLiteStore(path))


if __name__ == "__main__":
    asyncio.run(main())
