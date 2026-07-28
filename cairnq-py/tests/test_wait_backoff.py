"""wait/call polling. PROTOCOL.md describes a 100–500ms backoff; the implementation
polled at a flat 150ms, so waiting on a long task issued ~7 reads a second for its
whole duration."""

from __future__ import annotations

import asyncio

from cairnq._wait import next_poll_ms, poll_wait


def test_poll_interval_backs_off_to_the_ceiling():
    assert next_poll_ms(100, 500) == 150
    assert next_poll_ms(150, 500) == 225
    assert next_poll_ms(400, 500) == 500
    assert next_poll_ms(500, 500) == 500


async def test_waiting_on_a_slow_task_backs_off(client):
    """A one-second wait must not cost a read every 150ms."""
    task = await client.submit("never", {})
    reads = 0
    real_get = client.store.get

    async def counting_get(task_id):
        nonlocal reads
        reads += 1
        return await real_get(task_id)

    client.store.get = counting_get
    try:
        await asyncio.wait_for(client.wait(task.id, timeout_ms=1_200), timeout=3)
    except (asyncio.TimeoutError, Exception):
        pass
    finally:
        client.store.get = real_get

    # Flat 150ms would be ~8 reads; 100ms backing off by 1.5x is ~6 at most.
    assert reads <= 6, f"expected the poll interval to grow, got {reads} reads"
