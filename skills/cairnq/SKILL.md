---
name: cairnq
description: >-
  Use cairnq, the cross-language durable task runtime that coordinates through a
  shared database (SQLite for one host, Postgres for many). Covers the worker side
  (register handlers, run them), the API side (submit / call / get / cancel /
  retry / stats by id or business key), idempotency keys, retries, cooperative
  cancel, and the at-least-once limits. Trigger when code imports `cairnq` (Python
  or TypeScript), defines a Worker or handler, calls submit/call/getByKey, or the
  user mentions cairnq, tasks.db, a durable task queue, or an embedded SQLite job
  runtime.
---

# Using cairnq

## Mental model — read this first

Two processes that **never call each other**, coordinating only through one shared
database: the API `submit`s a task, the worker `claim`s and runs it, result and
state flow back through the store.

```
 API process  ─┐
               ├──  tasks.db  (SQLite, one host)  |  Postgres (many hosts)
 Worker       ─┘
```

Either side can be Python or TypeScript — only the **task name (a string)** and
JSON payload/result cross the store. `CairnQ.sqlite(path)` vs `.postgres(dsn)` is
the whole difference between backends. **TS is camelCase, Python snake_case**
(`getByKey`/`get_by_key`, `maxAttempts`/`max_attempts`); the examples below use
Python for the worker and TS for the API, but either language works on either side.

## Worker side

A handler **always** receives `(ctx, payload)` — `payload` is the whole dict.

```python
from cairnq import Worker

worker = Worker.sqlite("tasks.db", queues=["gpu"], concurrency=4, max_run_ms=600_000)

@worker.task                                # name defaults to the function name
async def summarize(ctx, payload):
    await ctx.progress(0.2, "reading")
    if await ctx.canceled():
        return
    return {"summary": await llm(payload["text"])}

worker.serve()                              # blocks; Ctrl-C / SIGTERM closes cleanly
```

TS: `worker.task("summarize", async (ctx, payload) => {…})`, or `worker.task(fn)`
→ name = `fn.name`. For a dotted/namespaced name pass it explicitly:
`@worker.task("summary.create")`.

**Options:** `queues` (`["default"]`), `concurrency` (1, also `serve(concurrency=…)`),
`lease_ms` (30s), `poll_interval_ms` (500ms), `retry_backoff_ms` /
`retry_backoff_max_ms` (1s doubling to 30s), `max_run_ms`, `on_error`. `serve()`
owns the process and its signals; `run()` / `background()` embed the worker in an
event loop you manage.

**`ctx`:** `payload`, `attempt`, `taskId`, `name`, `queue`, `metadata`, `rootId`,
`correlationId`; `await ctx.progress(value, msg)` (null = leave that field alone);
`await ctx.canceled()`; `await ctx.submit(name, payload)` for a child task
(parent/root/correlation wired automatically). Heartbeats are automatic — call
`ctx.heartbeat()` only if one step outlasts the lease.

`ctx.lostLease` / `ctx.lost_lease` goes true once another worker took the task over
after this lease expired: nothing written after that is recorded, so a handler with
real side effects should check it and return. For cancellable I/O, TS exposes
`ctx.signal` (`AbortSignal`), Python `ctx.lease_lost` (`asyncio.Event`).

## API side

```ts
import { CairnQ, isSucceeded } from "cairnq";
const tasks = CairnQ.sqlite("tasks.db");        // or CairnQ.postgres(dsn)

await tasks.submit("summarize", { text }, { key: `summary:${docId}` });   // returns at once
const result = await tasks.call("summarize", { text }, { waitTimeoutMs: 10_000 });

const t = await tasks.getByKey(`summary:${docId}`);    // predicates, not status strings
if (t && isSucceeded(t)) use(t.result);
```

Python exposes the same checks as properties: `t.succeeded` / `.failed` /
`.canceled` / `.running` / `.queued` / `.is_terminal`.

**Full surface**, each by `task_id` or business `key`: `submit`, `get` / `getByKey`,
`list`, `wait`, `call`, `cancel` / `cancelByKey`, `retry` / `retryByKey`, `purge`,
`stats`.

- **`submit`:** `key`, `queue` (`"default"`), `conflict` (`reuse` | `reject` |
  `replace`), `maxAttempts` (3), `priority`, `metadata`, `parentId`,
  `correlationId`, `runAtDelayMs`.
- **`list`:** `status`, `queue`, `name`, `rootId`, `correlationId`, `limit` (100),
  `offset`.
- **`retry(id, {resetAttempt: true})`** restarts from attempt 0 rather than
  spending the remaining `maxAttempts` budget.
- **`stats()`** → zero-filled counts per queue per status;
  `stats()["default"]["queued"]` is a backlog without listing rows.

## The non-obvious rules — where people go wrong

- **At-least-once, not exactly-once.** A worker can finish a side effect and crash
  before recording success; the task is redelivered once the lease expires. Key
  side effects on `ctx.taskId` or the business `key`.
- **`conflict: "reuse"` is *idempotent submit*, not "re-run if it failed".** It
  returns whatever task is under that key, *including a terminal `failed`/`canceled`
  one*. To force a fresh run: `replace` (new task, repoints the key) or `retry`
  (re-enqueue the same task).
- **Failing a task — pick retryable or not.** `TaskError(msg)` fails
  **permanently** (`retryable=False` by default, so deterministic bugs fail fast);
  `TaskError(msg, retryable=True)` retries with backoff up to `max_attempts`. Any
  **other** exception is treated as retryable.
- **A worker only claims what it has a handler for.** Queues need not partition
  work by name — a Python and a TypeScript worker can share `default`, each taking
  only its own tasks. A worker with no handlers claims nothing.
- **Cooperative cancel.** Cancelling a *queued* task is immediate; cancelling a
  *running* one only sets a flag, so the handler must check `ctx.canceled()` and
  return. Cancel then beats every other outcome — the result is discarded, a
  throwing attempt or expired lease still ends `canceled`, never redelivered.
- **A hung handler needs `max_run_ms`.** Heartbeats renew the lease for as long as
  a handler runs, so without a ceiling a wedged one holds its task `running` and
  its concurrency slot forever (cancel can't help — cooperative checks need a live
  handler). At the ceiling the attempt is abandoned and recorded as a retryable
  `handler_timeout`, so backoff and `max_attempts` still apply.
- **Progress belongs to the attempt.** Anything returning a task to `queued` — a
  retryable failure, `retry`, a crash — clears `progress`/`message`. Terminal tasks
  keep them, so a failed task still shows how far it got.
- **Nothing is deleted for you.** Terminal tasks stay until `purge(olderThanMs=…)`,
  which is bounded by `limit` (1000) per call — loop until it returns fewer than
  `limit`, on a schedule, or the database grows without bound.
- **Worker errors are silent unless you ask.** `on_error` / `onError` reports what
  the run loop survived (a claim that threw, a store write that failed while
  finalizing). Task *failures* go to the DB; these do not.
- **No in-DB auth.** Any process that can open the store has full access.

## SQLite vs Postgres

Same API, same canonical SQL, one shared conformance suite — the choice is purely
operational.

- **SQLite:** one host only. WAL needs every process on one machine and a local
  disk, never a network FS. Writes serialize, and in TS (`better-sqlite3` is
  synchronous) a contended write blocks the event loop up to `busy_timeout` (5s).
  Built for low-write, long-running AI jobs, not a high-throughput MQ.
- **Postgres:** multi-host, claims with `FOR UPDATE SKIP LOCKED`, and LISTEN/NOTIFY
  wakes idle workers plus `wait`/`call` the moment a task is queued or finishes
  (polling stays as the fallback). Needs the optional driver (`cairnq[postgres]` / `pg`).
- **Portability:** Postgres `jsonb` rejects NUL (`\u0000`) inside strings, SQLite
  accepts it — keep NUL out of payloads meant to run on both.

## Waiting (`call` / `wait`)

`call` returns the result on success, otherwise raises/throws `TaskFailed` (read
`.code` / `.message` / `.retryable` / `.details`; raw envelope on `.error`),
`TaskCanceled`, or `TaskTimeout` — on which **the task keeps running**, so follow
up via `.taskId` / `.task_id`.

## Typed tasks (optional, worth it as task types grow)

Define a name **once** and share the symbol on both ends — no `"summarize"` vs
`"sumarize"` drift, the editor finds every caller, and in TS payload and result are
fully typed:

```ts
import { defineTask } from "cairnq";
export const summarize = defineTask<{ text: string }, { summary: string }>("summarize");
worker.task(summarize, async (ctx, p) => ({ summary: await llm(p.text) }));
const { summary } = await tasks.call(summarize, { text });   // typed, no cast
```

Python: `TaskDef[dict, dict]("summarize")`, passed the same way to `@worker.task(…)`
and `call(…)`. Opt-in — every API still takes a plain name string, which is what a
cross-language caller uses.

## Reference

`cairnq-protocol/PROTOCOL.md` holds the contract and canonical SQL. Both SDKs load
that same SQL and pass one conformance suite against both dialects, so behaviour
matches across languages and backends.
