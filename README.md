# CairnQ

**A SQLite-first, cross-language, storage-centered durable task runtime.**

Your API process and your worker process never call each other. They coordinate
through a shared SQLite file: the API `submit`s tasks, the worker `claim`s and
runs them, results and state flow back through the database. Python and
TypeScript SDKs are interchangeable on either side.

```
 API process  ─┐
               ├──  tasks.db   (shared SQLite, WAL)
 Worker       ─┘
```

Built for embedded / local-first AI work: desktop apps, single-host deployments,
edge nodes, small internal services — an API server handing long jobs to AI
workers on the same machine. No broker, no server, no HTTP.

## Quick start

Worker (Python):

```python
from cairnq import Worker

worker = Worker.sqlite("tasks.db")

@worker.task                       # registered under the function name, "summarize"
async def summarize(ctx, payload):
    await ctx.progress(0.2, "reading")
    return {"summary": await llm.summarize(payload["text"])}

worker.serve()                     # blocking; Ctrl-C stops it and closes cleanly
```

> A handler always receives `(ctx, payload)` — the whole payload dict. Need a
> dotted/namespaced name (or one that differs from the function)? Pass it
> explicitly: `@worker.task("summary.create")`.

API (TypeScript) — submit and wait for the result:

```ts
import { CairnQ } from "cairnq";

const tasks = CairnQ.sqlite("tasks.db");
const result = await tasks.call("summarize", { text }, { waitTimeoutMs: 10_000 });
```

Or submit async and follow up by id / key:

```ts
const task = await tasks.submit("image.generate", { prompt }, {
  key: `user:${userId}:image:${requestId}`,  // business-stable, idempotent
  queue: "gpu",
  conflict: "reuse",
});
// later:
const t = await tasks.getByKey(key);
if (t && isSucceeded(t)) use(t.result);   // status predicates, no string matching
```

### Typed tasks (optional)

As the number of task types grows, define each one **once** and reference the same
symbol on both ends — the name lives in a single place (no `"summarize"` vs
`"sumarize"` drift), your editor autocompletes it and finds every caller, and in
TypeScript the payload and result are fully typed:

```ts
import { defineTask } from "cairnq";

export const summarize = defineTask<{ text: string }, { summary: string }>("summarize");

// worker — payload is { text: string }, the return is checked against { summary }
worker.task(summarize, async (ctx, payload) => ({ summary: await llm.summarize(payload.text) }));

// API — call() resolves to { summary: string }, no cast
const { summary } = await tasks.call(summarize, { text });
```

```python
from cairnq import TaskDef

summarize = TaskDef[dict, dict]("summarize")     # one symbol, shared by both ends

@worker.task(summarize)
async def handle(ctx, payload): ...

result = await tasks.call(summarize, {"text": text})
```

It's purely opt-in: every API still takes a plain name string, and a cross-language
caller keeps using the string (only the name crosses the database).

The worker and the API can be in **either language** — a TypeScript API can drive
a Python worker and vice-versa, because the only thing they share is the database
and a JSON protocol.

## What you get

- **`submit / get / wait / call / cancel / retry`**, by `task_id` or business `key`.
- **Key conflict strategies**: `reuse` (idempotent submit), `reject`, `replace`.
- **Lease-based claim** with heartbeat and automatic recovery — a crashed worker's
  task is redelivered after its lease expires.
- **At-least-once execution** (honestly — see below). Retries with backoff,
  `attempt` / `max_attempts`, queues, priority, progress, task chains
  (`parent_id` / `root_id` / `correlation_id`).
- **Cooperative cancel**: cancelling a running task sets a flag; when the handler
  returns after checking `ctx.canceled()`, the task finalizes as `canceled`
  (the result is discarded — cancel wins). Cancelling a queued task is immediate.
- **Handler-controlled failure**: raise `TaskError(..., retryable=False)` to fail
  a task permanently; any other thrown error is retried up to `max_attempts`.

### At-least-once, not exactly-once

A worker can finish an external side effect and then crash before recording
success; after the lease expires the task is redelivered. Make side effects
idempotent using `task_id` or `key`.

### `reuse` is idempotent submit, not "re-run if it failed"

`conflict: "reuse"` returns the task already recorded under that key — *whatever
its state*, including a terminal `failed`/`canceled` one. To force a new run use
`replace` (new task, repoints the key) or `retry` (re-enqueue the same task).

## Concurrency, limits & when *not* to use it

- **Same host only.** SQLite (WAL) needs all processes on one machine and a local
  disk — don't put the file on a network filesystem or share it across hosts.
  Multi-host is a future Postgres backend (designed-for via the `TaskStore` seam,
  not yet built).
- **One writer at a time.** `claim` / `submit` / worker writes are short, but heavy
  write concurrency serializes. Idle workers stay off the write lock (a read-only
  probe gates each poll); busy ones still contend. This is built for low-write,
  long-running AI work — not as a high-throughput message queue.
- **A contended write can block the process.** The TypeScript SDK
  (`better-sqlite3`) is synchronous: on lock contention it blocks the event loop
  up to `busy_timeout` (5s default). Fine at low write rates.
- **Full trust on the file.** There is no in-database authorization — any process
  that can open the file has full access. Protect it with OS permissions.

## Layout

```
cairnq-protocol/   schema + canonical SQLite SQL + conformance scenarios + PROTOCOL.md
cairnq-py/         Python SDK (aiosqlite)
cairnq-node/       TypeScript SDK (better-sqlite3)
conformance/       cross-language end-to-end orchestrator
```

The protocol is the contract. `cairnq-protocol/sql/sqlite/*.sql` holds the
**canonical state-transition statements**, loaded verbatim by both SDKs — that
(plus a shared conformance suite) is how Python and TypeScript are kept from
drifting. See [`cairnq-protocol/PROTOCOL.md`](cairnq-protocol/PROTOCOL.md).

## Develop

```bash
# Python SDK + conformance + store/worker tests
cd cairnq-py && uv sync --extra dev && uv run pytest

# TypeScript SDK + conformance + sdk tests
cd cairnq-node && pnpm install && pnpm test

# Cross-language: TS API -> Py worker, and Py API -> TS worker, over one tasks.db
cd conformance && pnpm test:cross-lang
```

No database server to start — the runtime is the SQLite file.
