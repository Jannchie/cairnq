from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from ._ids import now_ms
from .errors import TaskTimeout
from .models import Task
from .store.base import TaskStore

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
    read: Callable[[], Awaitable[Task | None]],
    wake: Callable[[Task | None, int], Awaitable[None]],
    subject: str,
    key: str | None,
    *,
    timeout_ms: int,
    poll_ms: int = DEFAULT_POLL_MS,
    max_poll_ms: int = MAX_POLL_MS,
) -> Task:
    """Poll `read` until it yields a terminal task, or the timeout elapses.

    `wake` is what the loop sleeps on between reads: a store with a push channel
    (Postgres) cuts it short when the task goes terminal, but the re-read is the
    source of truth either way, so a plain sleep is always a correct answer."""
    deadline = now_ms() + timeout_ms
    interval = poll_ms
    while True:
        task = await read()
        if task is not None and task.is_terminal:
            return task
        remaining = deadline - now_ms()
        if remaining <= 0:
            raise TaskTimeout(
                task.id if task is not None else subject,
                timeout_ms=timeout_ms,
                task=task,
                key=key,
            )
        await wake(task, min(interval, remaining))
        interval = next_poll_ms(interval, max_poll_ms)


async def poll_wait(
    store: TaskStore,
    task_id: str,
    *,
    timeout_ms: int,
    poll_ms: int = DEFAULT_POLL_MS,
    max_poll_ms: int = MAX_POLL_MS,
) -> Task:
    """Poll get() until the task is terminal or the timeout elapses. Returns the
    terminal Task (any status). Raises TaskTimeout, leaving the task running.

    `poll_ms` is the *first* interval; it backs off towards `max_poll_ms`."""

    async def wake(_task: Task | None, ms: int) -> None:
        await store.task_done_wake(task_id, ms)

    return await _poll(
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

    The key is re-resolved on every read, because that is what a key means: a
    pointer to the task that is *current* under it. A `replace` landing mid-wait
    moves the wait onto the new task rather than reporting the cancellation of
    the old one, and a key that points at nothing yet is simply not finished — it
    polls until something appears, the same way waiting on an id that does not
    exist yet does.

    There is nothing to subscribe to before the key resolves, so those naps are
    plain sleeps; once it resolves, the store's push channel applies as usual."""

    async def wake(task: Task | None, ms: int) -> None:
        if task is None:
            await asyncio.sleep(ms / 1000)
        else:
            await store.task_done_wake(task.id, ms)

    return await _poll(
        lambda: store.get_by_key(key),
        wake,
        key,
        key,
        timeout_ms=timeout_ms,
        poll_ms=poll_ms,
        max_poll_ms=max_poll_ms,
    )
