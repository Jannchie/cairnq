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
