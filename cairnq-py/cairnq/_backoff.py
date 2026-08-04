"""Retry backoff, in its own module because two callers need it.

The worker computes it when a handler's failure ends an attempt; TaskContext
computes it when a handler fails one task of a batch itself. Keeping it in
worker.py would make context.py import the module that imports it.
"""

from __future__ import annotations

DEFAULT_RETRY_BACKOFF_MS = 1_000
DEFAULT_RETRY_BACKOFF_MAX_MS = 30_000


def retry_delay_ms(attempt: int, *, base_ms: int, max_ms: int) -> int:
    """Exponential backoff for the next attempt of a task that just failed."""
    if base_ms <= 0:
        return 0
    return min(max_ms, base_ms * 2 ** max(0, attempt - 1))


def fail_delay_ms(attempt: int, *, retryable: bool, base_ms: int, max_ms: int) -> int:
    """The delay a `fail` write should carry. Not just the backoff: a permanent
    failure is never re-run, so it always delays 0. Both settlement paths — the
    worker's and a handler's `ctx.fail` — go through this, so they cannot end up
    backing off differently."""
    return retry_delay_ms(attempt, base_ms=base_ms, max_ms=max_ms) if retryable else 0
