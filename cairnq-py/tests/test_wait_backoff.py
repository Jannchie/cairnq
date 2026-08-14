"""wait/call polling. PROTOCOL.md describes a 100–500ms backoff; the implementation
polled at a flat 150ms, so waiting on a long task issued ~7 reads a second for its
whole duration."""

from __future__ import annotations

import asyncio

from cairnq._wait import next_poll_ms

from .helpers import succeed_next


def test_poll_interval_backs_off_to_the_ceiling():
    assert next_poll_ms(100, 500) == 150
    assert next_poll_ms(150, 500) == 225
    assert next_poll_ms(400, 500) == 500
    assert next_poll_ms(500, 500) == 500
    # int truncation must not pin tiny intervals: int(1 * 1.5) == 1 would
    # otherwise re-read the task as fast as possible for the whole timeout.
    assert next_poll_ms(1, 500) > 1
    assert next_poll_ms(0, 500) > 0


async def test_waiting_on_a_slow_task_backs_off(client):
    """A one-second wait must not cost a read every 150ms."""
    task = await client.submit("never", {})
    reads = 0
    real_probe = client.store.get_status

    async def counting_probe(task_id):
        nonlocal reads
        reads += 1
        return await real_probe(task_id)

    client.store.get_status = counting_probe
    try:
        await asyncio.wait_for(client.wait(task.id, timeout_ms=1_200), timeout=3)
    except (asyncio.TimeoutError, Exception):
        pass
    finally:
        client.store.get_status = real_probe

    # Flat 150ms would be ~8 reads; 100ms backing off by 1.5x is ~6 at most.
    assert reads <= 6, f"expected the poll interval to grow, got {reads} reads"


async def test_full_row_is_read_only_on_the_terminal_beat(client):
    """The poll loop's repeated read is the status-only probe; a 395KB payload
    must not be re-read and re-parsed on every beat of a five-minute queue."""
    task = await client.submit("job", {})
    full_reads = 0
    real_get = client.store.get

    async def counting_get(task_id):
        nonlocal full_reads
        full_reads += 1
        return await real_get(task_id)

    client.store.get = counting_get
    try:
        waiting = asyncio.ensure_future(client.wait(task.id, timeout_ms=3_000, poll_ms=20))
        await asyncio.sleep(0.15)
        await succeed_next(client, {"ok": True})
        done = await waiting
        assert done.succeeded
        assert done.result == {"ok": True}
    finally:
        client.store.get = real_get

    assert full_reads == 1


async def test_max_poll_ms_is_the_backoff_ceiling(client):
    """§7: call/wait must pass the ceiling through — with it stuck at 500ms, a
    known-slow task cannot trade detection latency for fewer reads. The naps the
    loop *asks for* are the assertion; the stub sleeps briefly and ends the task
    once the ask crosses the default ceiling (or gives up), so the test never
    waits the timeout out."""
    task = await client.submit("slow", {})
    naps: list[int] = []
    real_wake = client.store.task_done_wake

    async def recording_wake(task_id, ms):
        naps.append(ms)
        if ms > 500 or len(naps) > 30:
            await succeed_next(client)
        else:
            await real_wake(task_id, 10)

    client.store.task_done_wake = recording_wake
    try:
        done = await client.wait(task.id, timeout_ms=60_000, poll_ms=100, max_poll_ms=2_000)
        assert done.succeeded
    finally:
        client.store.task_done_wake = real_wake

    # Growing 1.5x from 100ms crosses the default 500ms ceiling only if the
    # caller's ceiling actually reached the loop.
    assert max(naps) > 500
    assert max(naps) <= 2_000
