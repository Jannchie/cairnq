"""Who owns the process's signals.

`serve()` is the standalone entry point — it owns the process, so it is the one
that may take SIGINT/SIGTERM. `run()` / `background()` embed the worker in someone
else's process (the API server it ships next to), where installing a handler
replaces the host's and never gives it back: after the worker was done, the host's
graceful shutdown never ran again and SIGTERM went nowhere until the orchestrator
gave up and sent SIGKILL."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import textwrap

from cairnq import CairnQ, Worker


async def test_background_leaves_the_host_signal_handlers_alone(db_path):
    loop = asyncio.get_running_loop()
    fired: list[int] = []
    loop.add_signal_handler(signal.SIGTERM, lambda: fired.append(1))
    try:
        worker = Worker.sqlite(db_path, poll_interval_ms=10)
        worker.task("job")(lambda ctx: {})
        async with worker.background():
            await asyncio.sleep(0.1)

        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(0.1)
        assert fired, "the worker replaced the host's SIGTERM handler and never restored it"
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def test_serve_still_stops_on_sigterm(tmp_path):
    """The other half of the same rule: moving the handlers into serve() must not
    cost serve() the shutdown it documents. Runs out-of-process because a real
    signal needs a process of its own."""
    db = str(tmp_path / "t.db")

    async def _seed() -> None:
        client = CairnQ.sqlite(db)
        await client.connect()
        await client.submit("job", {})
        await client.close()

    asyncio.run(_seed())

    # Readiness has to mean "the run loop is up", not "the script started" — a
    # signal delivered before serve() installs its handlers kills the process on
    # the default disposition and tells us nothing. Running a task proves it.
    script = textwrap.dedent(
        f"""
        from cairnq import Worker

        worker = Worker.sqlite({db!r}, poll_interval_ms=10)

        @worker.task("job")
        def job(ctx):
            print("ready", flush=True)
            return {{}}

        worker.serve()
        print("stopped", flush=True)
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "ready"
        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=10)
    except BaseException:
        proc.kill()
        raise

    assert proc.returncode == 0, f"serve() did not exit cleanly on SIGTERM: {out}"
    assert "stopped" in out, f"serve() never returned: {out}"
