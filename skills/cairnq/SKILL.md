---
name: cairnq
description: >-
  Use cairnq, the SQLite-first cross-language durable task runtime. Covers the
  worker side (register handlers, run them), the API side (submit / call / get /
  cancel / retry by id or business key), idempotency keys, retries, cooperative
  cancel, and the same-host / at-least-once limits. Trigger when code imports
  `cairnq` (Python or TypeScript), defines a Worker or handler, calls
  submit/call/getByKey, or the user mentions cairnq, tasks.db, a durable task
  queue, or an embedded SQLite job runtime.
---

# Using cairnq

## Mental model — read this first

Two processes that **never call each other**. They coordinate only through one
shared SQLite file. The API `submit`s a task; the worker `claim`s and runs it;
result and state flow back through the database.

```
 API process  ─┐
               ├──  tasks.db   (shared SQLite, WAL)
 Worker       ─┘
```

- Either side can be Python or TypeScript — the only thing that crosses the DB is
  the **task name (a string)** and JSON payload/result.
- Same host only, one local disk. Not a network broker, not high-throughput.
- TS methods are camelCase (`getByKey`, `maxAttempts`), Python is snake_case
  (`get_by_key`, `max_attempts`). Otherwise the two SDKs mirror each other.

## Worker side — define and run handlers

A handler **always** receives `(ctx, payload)`. `payload` is the whole dict —
destructure it yourself.

```python
from cairnq import Worker

worker = Worker.sqlite("tasks.db")          # add queues=["gpu"] to consume named queues

@worker.task                                # name defaults to the function name: "summarize"
async def summarize(ctx, payload):
    await ctx.progress(0.2, "reading")
    return {"summary": await llm(payload["text"])}

worker.serve()                              # blocks; Ctrl-C / SIGTERM closes cleanly
```

```ts
import { Worker } from "cairnq";

const worker = Worker.sqlite("tasks.db", { queues: ["gpu"] });
worker.task("summarize", async (ctx, payload) => {   // or worker.task(fn) → name = fn.name
  await ctx.progress(0.2, "reading");
  return { summary: await llm(payload.text) };
});
await worker.serve();
```

Need a dotted/namespaced name? Pass it explicitly: `@worker.task("summary.create")`.

**`ctx` gives you:** `ctx.payload`, `ctx.attempt`, `ctx.taskId`, `ctx.metadata`,
`ctx.rootId`, `ctx.correlationId`; `await ctx.progress(value, msg)`;
`await ctx.canceled()` (cooperative cancel check); `await ctx.submit(name, payload)`
(child task — parent/root/correlation wired automatically). Heartbeats are
automatic; you don't call `ctx.heartbeat()` unless a single step runs longer than
the lease.

`ctx.lostLease` / `ctx.lost_lease` (plus `ctx.signal`, an `AbortSignal`, in TS and
`ctx.lease_lost`, an `asyncio.Event`, in Python) goes true when this worker's lease
expired and another worker took the task over. Nothing you write after that is
recorded and the task is running elsewhere — a handler doing real side effects
should check it and return.

`progress` treats null as "leave it alone": `progress(0.5)` keeps the previous
message, `progress(null, "msg")` keeps the previous fraction.

## API side — submit and follow up

```python
from cairnq import CairnQ
tasks = CairnQ.sqlite("tasks.db")

# fire-and-forget, returns immediately:
t = await tasks.submit("summarize", {"text": text}, key=f"summary:{doc_id}")

# submit + wait for the result:
result = await tasks.call("summarize", {"text": text}, wait_timeout_ms=10_000)

# look it up later by id or business key (no status-string matching):
t = await tasks.get_by_key(f"summary:{doc_id}")
if t and t.succeeded:        # also .failed / .canceled / .running / .queued / .is_terminal
    use(t.result)
```

```ts
import { CairnQ, isSucceeded } from "cairnq";
const tasks = CairnQ.sqlite("tasks.db");

const t = await tasks.submit("summarize", { text }, { key: `summary:${docId}` });
const result = await tasks.call("summarize", { text }, { waitTimeoutMs: 10_000 });

const got = await tasks.getByKey(`summary:${docId}`);
if (got && isSucceeded(got)) use(got.result);   // also isFailed/isCanceled/isRunning/isQueued/isTerminal
```

**Full surface** (by `task_id` or business `key`): `submit`, `get` / `getByKey`,
`list`, `wait`, `call`, `cancel` / `cancelByKey`, `retry` / `retryByKey`, `purge`.

**`submit` options:** `key`, `queue` (default `"default"`), `conflict`
(`"reuse"` | `"reject"` | `"replace"`, default `reuse`), `maxAttempts` (default 3),
`priority`, `metadata`, `correlationId`, delayed start
(`runAtDelayMs` / `run_at_delay_ms`).

## The non-obvious rules — where people go wrong

- **At-least-once, not exactly-once.** A worker can finish a side effect and crash
  before recording success; after the lease expires the task is redelivered. Make
  side effects idempotent, keyed on `ctx.taskId` or the business `key`.
- **`conflict: "reuse"` is *idempotent submit*, not "re-run if it failed".** It
  returns the task already under that key — *whatever its state*, including a
  terminal `failed`/`canceled` one. To force a fresh run use `replace` (new task,
  repoints the key) or `retry` (re-enqueue the same task).
- **Failing a task — choose retryable or not.** Raise/throw `TaskError(msg)` →
  fails **permanently** (default `retryable=False`, so deterministic errors fail
  fast). `TaskError(msg, retryable=True)` → retried with backoff up to
  `max_attempts`. **Any other** exception → treated as retryable.
- **A worker only claims what it has a handler for.** Queues do not have to
  partition work by task name: two workers with different handler sets (say a
  Python one and a TypeScript one) can share `default` and each takes only its
  own. A worker with no handlers registered claims nothing.
- **Cooperative cancel.** Cancelling a *queued* task is immediate. Cancelling a
  *running* one only sets a flag — the handler must `if await ctx.canceled(): return`.
  On return the task finalizes as `canceled` and **the result is discarded** (cancel
  wins). Cancel wins over *every* outcome: if the attempt instead throws, or the
  worker dies and the lease expires, the task still ends `canceled` — never
  redelivered.
- **Nothing is deleted for you.** Terminal tasks stay forever until you call
  `purge(olderThanMs=…)` / `purge(older_than_ms=…)`. Schedule it, or the database
  grows without bound.
- **Worker errors are silent unless you ask.** Pass `onError` / `on_error` to the
  Worker to see what the run loop survived (a claim that threw, a store write that
  failed while finalizing). Task *failures* go to the DB; these do not.
- **Same host, one writer — on SQLite.** WAL needs all processes on one machine and
  a local disk — never a network FS. Writes serialize; in TS (`better-sqlite3` is
  synchronous) a contended write blocks the event loop up to `busy_timeout` (5s).
  Built for low-write, long-running AI jobs, not a high-throughput MQ. For
  multi-host, use `CairnQ.postgres(dsn)` / `Worker.postgres(dsn)` — same API.
- **No in-DB auth.** Any process that can open the file has full access — protect
  it with OS permissions.

## Errors when waiting (`call` / `wait`)

`call` returns the result on success, otherwise raises/throws:

- `TaskFailed` — the task ended in `failed`. Read `.code` / `.message` /
  `.retryable` / `.details` directly (raw envelope on `.error`).
- `TaskTimeout` — didn't finish in time. **The task keeps running**; `.task_id` /
  `.taskId` lets you follow up.
- `TaskCanceled` — ended in `canceled`.

## Typed tasks (optional, recommended as task types grow)

Define a name **once**, share the symbol on both ends — no `"summarize"` vs
`"sumarize"` drift, the editor finds every caller, and in TS payload + result are
fully typed:

```ts
import { defineTask } from "cairnq";
export const summarize = defineTask<{ text: string }, { summary: string }>("summarize");
worker.task(summarize, async (ctx, p) => ({ summary: await llm(p.text) }));
const { summary } = await tasks.call(summarize, { text });   // typed, no cast
```

```python
from cairnq import TaskDef
summarize = TaskDef[dict, dict]("summarize")
@worker.task(summarize)
async def handle(ctx, payload): ...
result = await tasks.call(summarize, {"text": text})
```

Purely opt-in — every API still takes a plain name string, which is what a
cross-language caller uses (only the name crosses the DB).

## Reference

Protocol contract and canonical SQL: `cairnq-protocol/PROTOCOL.md`. The Python
(`cairnq-py`) and TS (`cairnq-node`) SDKs load the same SQL and pass one shared
conformance suite, so behaviour matches across languages.
