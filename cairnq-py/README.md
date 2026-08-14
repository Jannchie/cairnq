# cairnq (Python)

SQLite-first, cross-language, storage-centered durable task runtime. The Python
SDK. API and worker processes coordinate only through a shared SQLite file.

```python
from cairnq import CairnQ, Worker

# Worker side — a handler always receives (ctx, payload).
worker = Worker.sqlite("tasks.db")

@worker.task                          # registered under the function name, "create_summary"
async def create_summary(ctx, payload):
    await ctx.progress(0.2, "reading")
    return {"summary": await llm.summarize(payload["text"])}

worker.serve()                        # blocking entry point; Ctrl-C closes cleanly

# API side (in your server) — submit returns immediately.
tasks = CairnQ.sqlite("tasks.db")
task = await tasks.submit("create_summary", {"text": text}, key=f"summary:{aid}")
```

`@worker.task` defaults the task name to the function's name. Pass a string for a
dotted/namespaced name: `@worker.task("summary.create")`.

Synchronous call (submit + wait):

```python
from cairnq import TaskFailed, TaskTimeout

try:
    result = await tasks.call("create_summary", {"text": text}, wait_timeout_ms=10_000)
except TaskFailed as e:
    log(e.code, e.message, e.retryable)   # envelope fields, no e.error["code"] digging
except TaskTimeout as e:
    # The task keeps running — resume the wait instead of submitting again.
    result = await tasks.wait(e.task_id, timeout_ms=60_000)
    # …or tasks.wait_by_key(key), from a process that never held the id.
```

Inspect a task by id/key without memorizing status strings:

```python
task = await tasks.get_by_key(key)
if task and task.succeeded:        # also .failed / .canceled / .running / .queued / .is_terminal
    use(task.result)
```

Optionally define a task once and share the symbol across both ends — the name
lives in one place (no string drift), and `call()` is typed as the task's result:

```python
from cairnq import TaskDef

summarize = TaskDef[dict, dict]("summarize")

@worker.task(summarize)            # registered under summarize.name
async def handle(ctx, payload): ...

result = await tasks.call(summarize, {"text": text})
```

Opt-in: every API still accepts a plain name string (cross-language callers use it).

## Running it in production

```python
worker = Worker.sqlite(
    "tasks.db",
    concurrency=4,            # handler calls at once; use max_in_flight_bytes to bound memory
    retry_backoff_ms=1_000,   # window doubles per attempt, capped at retry_backoff_max_ms (30s),
                              # jittered over its upper half; 0 disables
    on_error=lambda exc, info: log.warning("worker survived %s: %s", info, exc),
)

# Nothing else deletes rows, so give the client a retention policy — it sweeps
# terminal tasks in bounded batches for as long as the handle is open. A
# per-status mapping keeps each status on its own clock (statuses left out are
# never swept): spent results go in minutes, failures stay for diagnosis.
tasks = CairnQ.sqlite(
    "tasks.db",
    retention=Retention(older_than_ms={"succeeded": 300_000, "failed": 7 * 24 * 3600_000}),
)
```

A **sync** handler (`def`, not `async def`) is dispatched to a thread, so the
usual shape around a blocking GPU or HTTP call keeps the worker's event loop —
and with it every lease this worker holds — alive:

```python
@worker.task("score")
def score(ctx, payload):
    return {"score": model.forward(payload["image"])}  # blocking, off the loop
```

A handler that does real side effects should bail out when it loses its lease —
the task is already running on another worker and nothing it writes is recorded:

```python
@worker.task("long.job")
async def long_job(ctx, payload):
    for chunk in chunks:
        if ctx.lost_lease or await ctx.canceled():
            return
        await process(chunk)
```

## Multi-host

Same code, Postgres instead of the file — `CairnQ.postgres(dsn)` /
`Worker.postgres(dsn)`. Install with `pip install cairnq[postgres]`.

The protocol (schema + canonical SQL) lives in `../cairnq-protocol` and is shared
verbatim with the TypeScript SDK. See `../cairnq-protocol/PROTOCOL.md`.
