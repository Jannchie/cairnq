from __future__ import annotations

import asyncio

from ._ids import now_ms
from .errors import TaskTimeout
from .models import Task
from .store.base import TaskStore


async def poll_wait(
    store: TaskStore, task_id: str, *, timeout_ms: int, poll_ms: int = 150
) -> Task:
    """Poll get() until the task is terminal or the timeout elapses. Returns the
    terminal Task (any status). Raises TaskTimeout, leaving the task running."""
    deadline = now_ms() + timeout_ms
    while True:
        task = await store.get(task_id)
        if task is not None and task.is_terminal:
            return task
        remaining = deadline - now_ms()
        if remaining <= 0:
            raise TaskTimeout(task_id)
        await asyncio.sleep(min(poll_ms, remaining) / 1000)
