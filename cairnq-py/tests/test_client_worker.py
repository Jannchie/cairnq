import asyncio

import pytest

from cairnq import TaskError, Worker
from cairnq.errors import TaskFailed, TaskTimeout


async def test_call_timeout_keeps_task_running(client, db_path):
    # No worker for this name -> call should time out and the task stays queued.
    with pytest.raises(TaskTimeout) as excinfo:
        await client.call("unhandled", {}, wait_timeout_ms=300, poll_ms=50)
    task_id = excinfo.value.task_id
    assert task_id
    task = await client.get(task_id)
    assert task.status == "queued"


async def test_worker_end_to_end_with_call(client, db_path):
    worker = Worker.sqlite(db_path, queues=["default"], concurrency=2, poll_interval_ms=20)
    captured = {}

    @worker.task("sum")
    async def handle(ctx, payload):  # handler always receives (ctx, payload)
        captured["attempt"] = ctx.attempt
        await ctx.progress(0.5, "adding")
        return {"sum": payload["a"] + payload["b"]}

    async with worker.background():
        result = await client.call("sum", {"a": 2, "b": 3}, wait_timeout_ms=5000, poll_ms=20)

    assert result == {"sum": 5}
    assert captured["attempt"] == 1


async def test_worker_payload_signature(client, db_path):
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20)

    @worker.task("echo")
    async def handle(ctx, payload):  # single 'payload' param -> whole dict
        return {"echoed": payload}

    async with worker.background():
        result = await client.call("echo", {"hello": "world"}, wait_timeout_ms=5000, poll_ms=20)
    assert result == {"echoed": {"hello": "world"}}


async def test_worker_retries_then_succeeds(client, db_path):
    # retry_backoff_ms=0: this test is about the retry, not about waiting one out.
    worker = Worker.sqlite(
        db_path, queues=["default"], poll_interval_ms=20, lease_ms=5000, retry_backoff_ms=0
    )
    seen = []

    @worker.task("flaky")
    async def flaky(ctx):
        seen.append(ctx.attempt)
        if ctx.attempt < 2:
            raise RuntimeError("boom")
        return {"ok": True}

    async with worker.background():
        result = await client.call(
            "flaky", {}, max_attempts=3, wait_timeout_ms=5000, poll_ms=20
        )
    assert result == {"ok": True}
    assert seen == [1, 2]


async def test_child_task_chain(client, db_path):
    worker = Worker.sqlite(db_path, queues=["default"], concurrency=2, poll_interval_ms=20)

    @worker.task("parent")
    async def parent(ctx):
        child = await ctx.submit("child", {"v": 1})
        return {"child_id": child.id}

    @worker.task("child")
    async def child(ctx, payload):
        return {"v": payload["v"]}

    async with worker.background():
        parent_task = await client.submit("parent", {})
        parent_final = await client.wait(parent_task.id, timeout_ms=5000, poll_ms=20)
        child_id = parent_final.result["child_id"]
        child_final = await client.wait(child_id, timeout_ms=5000, poll_ms=20)

    assert child_final.status == "succeeded" and child_final.result == {"v": 1}
    child_task = await client.get(child_id)
    assert child_task.parent_id == parent_task.id
    assert child_task.root_id == parent_task.id  # parent is top-level -> its own root

    chain = await client.list(root_id=parent_task.id)
    assert {t.id for t in chain} == {parent_task.id, child_id}


async def test_cooperative_cancel_marks_canceled(client, db_path):
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20, lease_ms=5000)
    started = asyncio.Event()

    @worker.task("longjob")
    async def longjob(ctx):
        started.set()
        for _ in range(300):
            if await ctx.canceled():
                return  # cooperative exit
            await asyncio.sleep(0.01)
        return {"done": True}

    async with worker.background():
        task = await client.submit("longjob", {})
        await asyncio.wait_for(started.wait(), 2)
        await client.cancel(task.id)
        final = await client.wait(task.id, timeout_ms=3000, poll_ms=20)

    assert final.status == "canceled"  # not 'succeeded'
    assert final.result is None


async def test_task_error_is_non_retryable(client, db_path):
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20)
    attempts = []

    @worker.task("bad")
    async def bad(ctx):
        attempts.append(ctx.attempt)
        raise TaskError("bad input", code="bad_input", retryable=False)

    async with worker.background():
        with pytest.raises(TaskFailed) as excinfo:
            await client.call("bad", {}, max_attempts=3, wait_timeout_ms=3000, poll_ms=20)

    e = excinfo.value
    assert e.code == "bad_input" and e.message == "bad input" and e.retryable is False
    assert e.error["code"] == "bad_input"  # raw envelope still available for back-compat
    assert attempts == [1]  # failed permanently, not retried to max_attempts


async def test_bare_task_decorator_uses_function_name(client, db_path):
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20)

    @worker.task  # bare: registered under the function name "double"
    async def double(ctx, payload):
        return {"out": payload["n"] * 2}

    async with worker.background():
        result = await client.call("double", {"n": 21}, wait_timeout_ms=3000, poll_ms=20)
    assert result == {"out": 42}


async def test_payload_defaults_to_empty_dict(client, db_path):
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20)

    @worker.task  # ctx-only handler -> called with ctx alone
    async def ping(ctx):
        return {"pong": True}

    async with worker.background():
        result = await client.call("ping", wait_timeout_ms=3000, poll_ms=20)  # no payload arg
    assert result == {"pong": True}


async def test_background_keeps_injected_store_open(db_path):
    from cairnq import CairnQ
    from cairnq.store.sqlite import SQLiteStore

    store = SQLiteStore(db_path)  # one store shared by client and worker (mode A)
    client = CairnQ(store)
    worker = Worker(store, ["default"], poll_interval_ms=20)

    @worker.task
    async def ping(ctx):
        return {"ok": True}

    async with worker.background():
        result = await client.call("ping", wait_timeout_ms=3000, poll_ms=20)
    assert result == {"ok": True}

    # The store was injected, not created by Worker.sqlite, so background() must
    # leave it open — this query would fail if the connection had been closed.
    again = await client.submit("ping", {})
    assert again.queued
    await store.close()


async def test_task_def_shares_name_across_worker_and_client(client, db_path):
    from cairnq import TaskDef

    greet = TaskDef[dict, dict]("greet")  # one symbol referenced on both ends
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20)

    @worker.task(greet)  # registered under greet.name, no string repeated
    async def handle(ctx, payload):
        return {"msg": f"hi {payload['who']}"}

    async with worker.background():
        result = await client.call(greet, {"who": "ada"}, wait_timeout_ms=3000, poll_ms=20)
    assert result == {"msg": "hi ada"}

    # A plain-string submit hits the same handler — the TaskDef is just the name.
    again = await client.submit("greet", {"who": "x"})
    assert again.name == "greet"


async def test_task_status_predicates(client, db_path):
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20)

    @worker.task
    async def ok(ctx):
        return {"done": True}

    task = await client.submit("ok", {})
    assert task.queued and not task.is_terminal and not task.succeeded

    async with worker.background():
        final = await client.wait(task.id, timeout_ms=3000, poll_ms=20)

    assert final.succeeded and final.is_terminal
    assert not final.failed and not final.canceled and not final.queued
