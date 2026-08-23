from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from .errors import QueueFull

if TYPE_CHECKING:
    from .store.base import TaskStore

#: Most tasks a producer may enqueue on one probe's word.
#:
#: The gate probes only when its headroom runs out, so this is what the check
#: costs amortized: one bounded index read per MAX_GRANT submits. It also bounds
#: how far the limit can be overshot — see QueueDepthGate on why several
#: producers make this a soft limit, and why that overshoot is
#: (N-1) * MAX_GRANT rather than unbounded.
MAX_GRANT = 64

# Named for probing, not polling: _wait.py exports DEFAULT_POLL_MS / MAX_POLL_MS
# for the get() loop behind wait(), an order of magnitude tighter and answering a
# different question. Two constants of the same name in one SDK would be read as
# one policy.
INITIAL_PROBE_INTERVAL_MS = 250
MAX_PROBE_INTERVAL_MS = 5_000
DEFAULT_MAX_WAIT_MS = 600_000

#: Per-queue depth limits. An int applies one limit to every queue; a dict gates
#: only the queues it names and leaves the rest unbounded.
QueueDepthLimit = int | dict[str, int]


class QueueDepthGate:
    """Blocks `submit` while a queue is at its depth limit.

    Without one of these a producer that outruns its workers is only bounded by
    disk: the backlog grows, every task's queue wait grows with it, and the
    failure is a database that filled up rather than a producer that slowed
    down. A queue is the wrong place to buffer an overload — pushing back on the
    producer is the point.

    **A soft limit under several producers.** The check is a read followed by a
    write that other producers can interleave with, and each holds its own grant,
    so N producers can overshoot the limit by up to (N-1) * MAX_GRANT tasks. Made
    exact it would need the depth check inside insert_task's transaction, which
    puts an unbounded-scan predicate on the hot path of every submit and turns
    concurrent submits into lock contention — a steep price for a bound whose
    whole purpose is approximate. Size the limit for the pushback you want, not
    as a capacity assertion.
    """

    def __init__(
        self,
        store: TaskStore,
        max_queue_depth: QueueDepthLimit,
        *,
        max_queue_wait_ms: int = DEFAULT_MAX_WAIT_MS,
    ):
        self._store = store
        self._limits = max_queue_depth
        self._max_wait_ms = max_queue_wait_ms
        #: Remaining grant per queue: submits allowed before the next probe.
        self._headroom: dict[str, int] = {}
        #: In-flight probe per queue, so concurrent submits share one read rather
        #: than each issuing their own against a queue already known to be full.
        self._probing: dict[str, asyncio.Future[None]] = {}

        if isinstance(self._limits, int):
            self._validate("*", self._limits)
        else:
            for queue, limit in self._limits.items():
                self._validate(queue, limit)

    @staticmethod
    def _validate(queue: str, limit: int) -> None:
        # A limit of 0 would block every submit forever, which is never what a
        # caller means; catching it here beats a first submit that hangs for
        # max_queue_wait_ms and then raises.
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError(f"max_queue_depth for {queue!r} must be an int >= 1, got {limit!r}")

    def limit_for(self, queue: str) -> int | None:
        """The limit for `queue`, or None when it is not gated."""
        if isinstance(self._limits, int):
            return self._limits
        return self._limits.get(queue)

    async def acquire(self, queue: str) -> None:
        """Consume one unit of headroom for `queue`, waiting for room if it is
        full. Returns immediately for an ungated queue. Raises QueueFull on
        timeout, having enqueued nothing."""
        limit = self.limit_for(queue)
        if limit is None:
            return

        started_at = time.monotonic()
        wait_ms = INITIAL_PROBE_INTERVAL_MS
        while True:
            left = self._headroom.get(queue, 0)
            if left > 0:
                self._headroom[queue] = left - 1
                return
            await self._probe(queue, limit)
            if self._headroom.get(queue, 0) > 0:
                continue

            waited_ms = int((time.monotonic() - started_at) * 1000)
            if waited_ms >= self._max_wait_ms:
                raise QueueFull(queue, limit, waited_ms)
            # Back off: a queue at its limit will not drain within one poll
            # interval, and re-probing tightly adds read load to a database
            # already behind.
            await asyncio.sleep(min(wait_ms, self._max_wait_ms - waited_ms) / 1000)
            wait_ms = min(wait_ms * 2, MAX_PROBE_INTERVAL_MS)

    async def _probe(self, queue: str, limit: int) -> None:
        """Refresh `queue`'s grant from the store, at most one probe in flight.

        Callers re-read `_headroom` afterwards rather than using a returned
        value: only the task that started the probe writes the grant, so waiters
        that joined it cannot overwrite the units already handed out. The join is
        shielded so one waiter's cancellation does not cancel the shared read out
        from under the others."""
        task = self._probing.get(queue)
        if task is None:
            task = asyncio.ensure_future(self._refresh(queue, limit))
            self._probing[queue] = task
            task.add_done_callback(lambda _t, q=queue: self._probing.pop(q, None))
        await asyncio.shield(task)

    async def _refresh(self, queue: str, limit: int) -> None:
        headroom = await self._store.queue_depth(queue, limit)
        self._headroom[queue] = min(headroom, MAX_GRANT)
