"""Retry backoff. The failure this pins: the worker recorded every failure with
delay_ms=0, so a task that keeps failing was re-claimed at poll speed and burned
through max_attempts in milliseconds — while README and PROTOCOL both promised
"retries with backoff"."""

from __future__ import annotations

from cairnq import Worker
from cairnq._ids import now_ms
from cairnq.worker import retry_delay_ms

from .helpers import wait_for


def test_retry_delay_window_doubles_per_attempt_and_is_capped():
    """The jitter window, pinned at its top end (rand just under 1)."""
    top = lambda: 1.0  # noqa: E731 — the open end of [0, 1)
    assert retry_delay_ms(1, base_ms=1_000, max_ms=30_000, rand=top) == 1_000
    assert retry_delay_ms(2, base_ms=1_000, max_ms=30_000, rand=top) == 2_000
    assert retry_delay_ms(3, base_ms=1_000, max_ms=30_000, rand=top) == 4_000
    assert retry_delay_ms(20, base_ms=1_000, max_ms=30_000, rand=top) == 30_000


def test_retry_delay_jitters_over_the_upper_half_of_the_window():
    """Equal jitter: never below half the window, never above it. The floor is
    what keeps jitter from turning a long backoff into a fast retry; the spread
    is what stops a fleet capped at max_ms from retrying on one beat."""
    assert retry_delay_ms(3, base_ms=1_000, max_ms=30_000, rand=lambda: 0.0) == 2_000
    assert retry_delay_ms(3, base_ms=1_000, max_ms=30_000, rand=lambda: 0.5) == 3_000

    seen = {retry_delay_ms(20, base_ms=1_000, max_ms=30_000) for _ in range(200)}
    assert len(seen) > 1, "a capped backoff must not be a single deterministic beat"
    assert all(15_000 <= d <= 30_000 for d in seen)


def test_retry_delay_of_zero_disables_backoff():
    assert retry_delay_ms(3, base_ms=0, max_ms=30_000) == 0


async def test_failed_attempt_is_requeued_into_the_future(client, db_path):
    worker = Worker.sqlite(
        db_path, queues=["default"], poll_interval_ms=20, retry_backoff_ms=2_000
    )

    @worker.task("flaky")
    async def flaky(ctx):
        raise RuntimeError("boom")

    async with worker.background():
        t = await client.submit("flaky", {}, max_attempts=3)
        cur = None

        async def requeued() -> bool:
            nonlocal cur
            cur = await client.get(t.id)
            return cur.queued and cur.attempt >= 1

        await wait_for(requeued)

    assert cur is not None and cur.queued and cur.attempt == 1
    assert cur.run_at_ms > now_ms() + 100, "a failed attempt must wait out the backoff"
