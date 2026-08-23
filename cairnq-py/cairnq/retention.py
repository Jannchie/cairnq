from __future__ import annotations

import asyncio

from .store.base import TaskStore

#: Sweep every hour unless asked otherwise — often enough that a queue with a day
#: of retention never carries more than an hour of extra rows, rare enough that
#: the sweep is invisible next to the task traffic.
DEFAULT_INTERVAL_MS = 3_600_000
#: Rows per purge statement. The same bound `purge` defaults to: big enough that a
#: backlog drains in few statements, small enough that each is a short write.
DEFAULT_LIMIT = 1_000


class RetentionSweeper:
    """Deletes terminal tasks on a schedule, for as long as the handle is open —
    `purge(older_than_ms)` on a timer, which is the whole mechanism.

    `purge` exists because nothing else in CairnQ removes rows, and a queue whose
    payloads carry real data — an image, a document, a batch of embeddings — turns
    that into a disk leak measured in gigabytes per backfill. Every deployment
    that runs longer than a demo needs the sweep; leaving it to an external
    scheduler means the leak is the default and remembering is the opt-in.
    A deployment whose retention is tiered (per queue, per status) calls `purge`
    with those filters from its own scheduler instead.

    It sweeps in bounded batches with a yield between them, so draining a backlog
    that accumulated while nothing was sweeping stays a sequence of short writes
    rather than one long one — on SQLite that matters, since a long write holds
    the single write lock against every producer and worker on the file.
    """

    def __init__(
        self, store: TaskStore, older_than_ms: int, *, interval_ms: int = DEFAULT_INTERVAL_MS
    ):
        if older_than_ms < 0:
            raise ValueError(f"retention_ms must be >= 0, got {older_than_ms}")
        if interval_ms < 1:
            raise ValueError(f"retention interval_ms must be >= 1, got {interval_ms}")
        self._store = store
        self._older_than_ms = older_than_ms
        self._interval_ms = interval_ms
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        """Begin sweeping. Idempotent, and a no-op outside a running loop.

        Called by the store when it connects rather than by whoever built the
        handle: scheduling needs a live event loop, and a handle constructed at
        import time has none. See TaskStore.use_retention."""
        if self._task is not None:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop sweeping and wait for the sweep in flight, if any."""
        if self._task is None:
            return
        self._stop.set()
        task, self._task = self._task, None
        await task
        # Cleared now that the loop is provably gone. `_stop` is how a sweep in
        # flight cuts itself short, so leaving it set silently truncated a later
        # on-demand sweep() — a supported call — to its first batch, and made a
        # start() after a stop() exit its loop on the first check. Mirrors
        # RetentionSweeper.stop in the TypeScript SDK.
        self._stop.clear()

    async def _run(self) -> None:
        # Sleep first: a process that restarts often would otherwise purge on
        # every boot, which is a write burst exactly when the store is busiest.
        while not self._stop.is_set():
            await self._sleep(self._interval_ms)
            if self._stop.is_set():
                return
            try:
                await self.sweep()
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 — swallowed, never fatal
                # A purge that failed because the database was busy is not a
                # reason to stop retaining — the next sweep runs on schedule
                # regardless.
                pass

    async def sweep(self) -> int:
        """Delete everything past the cutoff now, in bounded batches, and return
        how many rows went. The scheduled loop calls this; call it directly to
        drain on demand — after a backfill, or from a maintenance command."""
        deleted = 0
        while True:
            ids = await self._store.purge(
                older_than_ms=self._older_than_ms, limit=DEFAULT_LIMIT
            )
            deleted += len(ids)
            if self._stop.is_set():
                return deleted
            if len(ids) < DEFAULT_LIMIT:
                return deleted
            # Hand the loop back between batches: a large drain must not starve
            # the submits and claims sharing this process.
            await asyncio.sleep(0)

    async def _sleep(self, ms: int) -> None:
        """Sleep, interruptible by stop() — so closing a handle need not wait out
        a whole interval."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=ms / 1000)
        except (asyncio.TimeoutError, TimeoutError):
            pass
