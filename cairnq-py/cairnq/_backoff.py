"""Retry backoff, in its own module because two callers need it.

The worker computes it when a handler's failure ends an attempt; TaskContext
computes it when a handler fails one task of a batch itself. Keeping it in
worker.py would make context.py import the module that imports it.
"""

from __future__ import annotations

import random
from typing import Callable

DEFAULT_RETRY_BACKOFF_MS = 1_000
DEFAULT_RETRY_BACKOFF_MAX_MS = 30_000


def retry_delay_ms(
    attempt: int, *, base_ms: int, max_ms: int, rand: Callable[[], float] = random.random
) -> int:
    """Exponential backoff with equal jitter: the window doubles per attempt up
    to `max_ms`, and the delay lands uniformly in its upper half, `[w/2, w)`.

    The jitter is what keeps a fleet from retrying in lockstep. Failures align
    when the downstream fails fast enough that a whole concurrency batch raises
    at once (connection refused, DNS gone), and capped exponential backoff then
    *preserves* that alignment — once every task sits at `max_ms`, they all
    retry on the same beat forever. Spreading over half the window breaks it;
    keeping the lower half as a floor means jitter never shortens the wait to
    less than half of what plain exponential backoff would have asked for.

    `rand` is injected so tests can pin an exact delay.
    """
    if base_ms <= 0:
        return 0
    window = min(max_ms, base_ms * 2 ** max(0, attempt - 1))
    floor = window // 2
    return floor + int(rand() * (window - floor))


def fail_delay_ms(
    attempt: int,
    *,
    retryable: bool,
    base_ms: int,
    max_ms: int,
    rand: Callable[[], float] = random.random,
) -> int:
    """The delay a `fail` write should carry. Not just the backoff: a permanent
    failure is never re-run, so it always delays 0. Both settlement paths — the
    worker's and a handler's `ctx.fail` — go through this, so they cannot end up
    backing off differently."""
    if not retryable:
        return 0
    return retry_delay_ms(attempt, base_ms=base_ms, max_ms=max_ms, rand=rand)
