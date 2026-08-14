from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from ._ids import now_ms
from .errors import TaskTimeout
from .models import Task, TaskRef
from .store.base import TaskStore

DEFAULT_WAIT_TIMEOUT_MS = 30_000
DEFAULT_POLL_MS = 100
MAX_POLL_MS = 500
_GROWTH = 1.5


def next_poll_ms(current: int, max_ms: int) -> int:
    """Grow the polling interval towards the ceiling.

    wait() has no idea whether the task takes 50ms or an hour. Starting tight
    keeps short tasks snappy; growing keeps a long wait from costing a read every
    100ms for its whole duration. The +1 keeps truncation from pinning tiny
    intervals: int(1 * 1.5) == 1 would otherwise never grow past 1."""
    return min(max_ms, max(current + 1, int(current * _GROWTH)))


async def _poll(
    probe: Callable[[], Awaitable[TaskRef | None]],
    read: Callable[[], Awaitable[Task | None]],
    wake: Callable[[TaskRef | None, int], Awaitable[None]],
    subject: str,
    key: str | None,
    *,
    timeout_ms: int,
    poll_ms: int = DEFAULT_POLL_MS,
    max_poll_ms: int = MAX_POLL_MS,
) -> Task:
    """Poll `probe` until it reports a terminal status, then return the full task
    via `read`; or raise once the timeout elapses.

    The loop's repeated read is the status-only `probe` (see get_status.sql): a
    waiting caller asks nothing but "is it finished yet", and re-reading the
    whole row would drag the payload back — and re-parse it — on every beat for
    the life of the wait. The full row is read once, when the probe turns
    terminal or, on the timeout beat, for the error's snapshot. Between the
    probe and that read the row can vanish (purge) or the key repoint
    (`replace`); a read that comes back empty or non-terminal is simply not
    finished, and the loop keeps polling.

    `wake` is what the loop sleeps on between reads: a store with a push channel
    (Postgres) cuts it short when the task goes terminal, but the re-probe is the
    source of truth either way, so a plain sleep is always a correct answer."""
    deadline = now_ms() + timeout_ms
    interval = poll_ms
    while True:
        ref = await probe()
        remaining = deadline - now_ms()
        # The one full-read site: when the probe says finished, or on the
        # timeout beat for the error's stuck-in-what-state snapshot. No ref
        # means no row, so there is nothing for a read to add to either case.
        task = await read() if ref is not None and (ref.is_terminal or remaining <= 0) else None
        if task is not None and task.is_terminal:
            return task
        if remaining <= 0:
            raise TaskTimeout(
                ref.id if ref is not None else subject,
                timeout_ms=timeout_ms,
                task=task,
                key=key,
            )
        await wake(ref, min(interval, remaining))
        interval = next_poll_ms(interval, max_poll_ms)


async def poll_wait(
    store: TaskStore,
    task_id: str,
    *,
    timeout_ms: int,
    poll_ms: int = DEFAULT_POLL_MS,
    max_poll_ms: int = MAX_POLL_MS,
) -> Task:
    """Poll the task's status until terminal or the timeout elapses. Returns the
    terminal Task (any status). Raises TaskTimeout, leaving the task running.

    `poll_ms` is the *first* interval; it backs off towards `max_poll_ms`."""

    async def wake(_ref: TaskRef | None, ms: int) -> None:
        await store.task_done_wake(task_id, ms)

    return await _poll(
        lambda: store.get_status(task_id),
        lambda: store.get(task_id),
        wake,
        task_id,
        None,
        timeout_ms=timeout_ms,
        poll_ms=poll_ms,
        max_poll_ms=max_poll_ms,
    )


async def poll_wait_by_key(
    store: TaskStore,
    key: str,
    *,
    timeout_ms: int,
    poll_ms: int = DEFAULT_POLL_MS,
    max_poll_ms: int = MAX_POLL_MS,
) -> Task:
    """The same wait, following a key instead of an id.

    The key is re-resolved on every probe, because that is what a key means: a
    pointer to the task that is *current* under it. A `replace` landing mid-wait
    moves the wait onto the new task rather than reporting the cancellation of
    the old one, and a key that points at nothing yet is simply not finished — it
    polls until something appears, the same way waiting on an id that does not
    exist yet does.

    There is nothing to subscribe to before the key resolves, so those naps are
    plain sleeps; once it resolves, the store's push channel applies as usual."""

    async def wake(ref: TaskRef | None, ms: int) -> None:
        if ref is None:
            await asyncio.sleep(ms / 1000)
        else:
            await store.task_done_wake(ref.id, ms)

    return await _poll(
        lambda: store.get_status_by_key(key),
        lambda: store.get_by_key(key),
        wake,
        key,
        key,
        timeout_ms=timeout_ms,
        poll_ms=poll_ms,
        max_poll_ms=max_poll_ms,
    )
