from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable


async def wait_for(
    cond: Callable[[], bool | Awaitable[bool]], timeout_s: float = 3.0
) -> None:
    """Wait until `cond` holds, or the timeout elapses — the timeout is not a
    failure here, it just stops waiting so the test's own assertion reports what
    went wrong instead of an opaque timeout."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        result = cond()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.01)


async def all_terminal(client, ids) -> bool:
    """Whether every id has reached a terminal state. The predicate worker tests
    wait on, so `wait_for(lambda: all_terminal(client, ids))` reads as the thing
    it is rather than being respelled per file."""
    for task_id in ids:
        task = await client.get(task_id)
        if task is None or not task.is_terminal:
            return False
    return True
