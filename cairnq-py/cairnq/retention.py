from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from .models import TaskStatus
from .store.base import TaskStore, validate_purge_input

#: Sweep every hour unless asked otherwise — often enough that a queue with a day
#: of retention never carries more than an hour of extra rows, rare enough that
#: the sweep is invisible next to the task traffic.
DEFAULT_INTERVAL_MS = 3_600_000
#: Rows per purge statement. The same bound `purge` defaults to: big enough that a
#: backlog drains in few statements, small enough that each is a short write.
DEFAULT_LIMIT = 1_000


@dataclass(frozen=True)
class RetentionRule:
    """One "these rows may go after this long" statement. Each field left out
    widens what the rule covers; `older_than_ms` is the only required one.

    A rule is one `purge` call's filters, so the fields are exactly `purge`'s —
    deliberately, since a rule the sweeper can express but the store cannot
    enforce would be a lie about what is being deleted.
    """

    older_than_ms: int
    #: Only this queue. None means every queue.
    queue: str | None = None
    #: Only this terminal status. None means all three.
    status: TaskStatus | None = None
    #: Only this task name. None means every name.
    name: str | None = None


@dataclass(frozen=True)
class Retention:
    """How long terminal tasks are kept, and how often that is enforced.

    `older_than_ms` is required: there is no safe default for how long someone
    else's results stay readable. Three forms, widening as the deployment does:

    - An int keeps every terminal row the same time.
    - A per-status mapping tiers by outcome — a succeeded row is spent once its
      result is consumed, a failed one is worth keeping for diagnosis:
      `{"succeeded": 300_000, "failed": 86_400_000}`. A status left out of the
      mapping is never swept — granular retention is an explicit statement of
      what may go, not a default for what wasn't named.
    - A sequence of `RetentionRule` tiers by anything `purge` can filter on,
      which is what a store shared by two workloads needs — the recommended way
      for two languages to coordinate is one installation, and an RPC queue read
      once has nothing in common with a durable queue kept for a week::

          [RetentionRule(queue="rpc", older_than_ms=300_000),
           RetentionRule(queue="jobs", status="failed", older_than_ms=604_800_000)]

      Rules are independent, each its own sweep — nothing a rule does not match
      is swept, and rules that overlap simply delete the same row once.

    `on_error` is called for a sweep that threw — the next sweep runs on schedule
    regardless, since a purge that failed because the database was busy is not a
    reason to stop retaining, so without it a store quietly stops being swept. It
    must not raise.
    """

    older_than_ms: int | Mapping[TaskStatus, int] | Sequence[RetentionRule]
    interval_ms: int = DEFAULT_INTERVAL_MS
    limit: int = DEFAULT_LIMIT
    on_error: Callable[[BaseException], None] | None = None

    def __post_init__(self) -> None:
        rules = self.rules()
        # An empty mapping or sequence retains nothing and sweeps nothing —
        # almost certainly a bug upstream, so refuse it rather than silently
        # never purging.
        if not rules:
            raise ValueError("retention older_than_ms must name at least one rule")
        if self.interval_ms < 1:
            raise ValueError(f"retention interval_ms must be >= 1, got {self.interval_ms}")
        # Fail fast on the store's own purge rules (terminal status, cutoff and
        # limit bounds): the sweep runs an hour from now, and its errors only
        # surface via on_error.
        for rule in rules:
            validate_purge_input(
                older_than_ms=rule.older_than_ms, status=rule.status, limit=self.limit
            )

    def rules(self) -> list[RetentionRule]:
        """The rules one sweep purges. The int and the per-status mapping are the
        sequence form's common cases spelled shorter, so they are widened here
        rather than handled separately downstream."""
        spec = self.older_than_ms
        if isinstance(spec, int):
            return [RetentionRule(older_than_ms=spec)]
        if isinstance(spec, Mapping):
            return [RetentionRule(older_than_ms=ms, status=s) for s, ms in spec.items()]
        return list(spec)


class RetentionSweeper:
    """Deletes terminal tasks on a schedule, for as long as the handle is open.

    `purge` exists because nothing else in CairnQ removes rows, and a queue whose
    payloads carry real data — an image, a document, a batch of embeddings — turns
    that into a disk leak measured in gigabytes per backfill. Every deployment
    that runs longer than a demo needs the sweep; leaving it to an external
    scheduler means the leak is the default and remembering is the opt-in.

    It sweeps in bounded batches with a yield between them, so draining a backlog
    that accumulated while nothing was sweeping stays a sequence of short writes
    rather than one long one — on SQLite that matters, since a long write holds
    the single write lock against every producer and worker on the file.
    """

    def __init__(self, store: TaskStore, retention: Retention):
        self._store = store
        self._retention = retention
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
            await self._sleep(self._retention.interval_ms)
            if self._stop.is_set():
                return
            try:
                await self.sweep()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 — reported, never fatal
                if self._retention.on_error is not None:
                    # A reporting hook must never take the sweep down with it —
                    # the same rule the worker's on_error follows.
                    with contextlib.suppress(BaseException):
                        self._retention.on_error(exc)

    async def sweep(self) -> int:
        """Delete everything past the cutoff now, in bounded batches, and return
        how many rows went. The scheduled loop calls this; call it directly to
        drain on demand — after a backfill, or from a maintenance command."""
        limit = self._retention.limit
        deleted = 0
        for rule in self._retention.rules():
            while True:
                ids = await self._store.purge(
                    older_than_ms=rule.older_than_ms,
                    queue=rule.queue,
                    status=rule.status,
                    name=rule.name,
                    limit=limit,
                )
                deleted += len(ids)
                if self._stop.is_set():
                    return deleted
                if len(ids) < limit:
                    break
                # Hand the loop back between batches: a large drain must not
                # starve the submits and claims sharing this process.
                await asyncio.sleep(0)
        return deleted

    async def _sleep(self, ms: int) -> None:
        """Sleep, interruptible by stop() — so closing a handle need not wait out
        a whole interval."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=ms / 1000)
        except (asyncio.TimeoutError, TimeoutError):
            pass
