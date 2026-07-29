from __future__ import annotations

import asyncio

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
    100ms for its whole duration."""
    return min(max_ms, int(current * _GROWTH))


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
    deadline = now_ms() + timeout_ms
    interval = poll_ms
    while True:
        task = await store.get(task_id)
        if task is not None and task.is_terminal:
            return task
        remaining = deadline - now_ms()
        if remaining <= 0:
            raise TaskTimeout(task_id, timeout_ms=timeout_ms, task=task)
        # A store with a push channel (Postgres) cuts the sleep short when the
        # task goes terminal; the re-get above stays the source of truth.
        await store.task_done_wake(task_id, min(interval, remaining))
        interval = next_poll_ms(interval, max_poll_ms)
