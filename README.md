# CairnQ

**A SQLite-first, cross-language, storage-centered durable task runtime — and the
same code on Postgres once you outgrow one host.**

Your API process and your worker process never call each other. They coordinate
through a shared database: the API `submit`s tasks, the worker `claim`s and runs
them, results and state flow back through the store. Python and TypeScript SDKs
are interchangeable on either side.

```
 API process  ─┐
               ├──  tasks.db  (SQLite, one host)  |  Postgres (many hosts)
 Worker       ─┘
```

Built for embedded / local-first AI work: desktop apps, single-host deployments,
edge nodes, small internal services — an API server handing long jobs to AI
workers on the same machine. No broker, no HTTP — and on SQLite, no server at all.

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
  task is redelivered after its lease expires. A worker only claims tasks it has a
  handler for, so workers with different handler sets share a queue safely — which
  is what makes a Python API and a TypeScript worker on one `default` queue work.
- **At-least-once execution** (honestly — see below). Retries with exponential
  backoff (1s doubling to 30s by default), `attempt` / `max_attempts`, queues,
  priority, progress, task chains (`parent_id` / `root_id` / `correlation_id`).
- **Cooperative cancel**: cancelling a running task sets a flag; when the handler
  returns after checking `ctx.canceled()`, the task finalizes as `canceled` (the
  result is discarded — cancel wins). Cancelling a queued task is immediate. Cancel
  outranks every other outcome: a cancelled task is never redelivered, even if its
  attempt failed retryably or its worker crashed.
- **Handler-controlled failure**: raise `TaskError(..., retryable=False)` to fail
  a task permanently; any other thrown error is retried up to `max_attempts`.
- **Hung-handler ceiling**: optional `max_run_ms` / `maxRunMs` bounds one
  attempt's wall clock — the heartbeat otherwise renews a hung handler's lease
  forever. At the ceiling the worker abandons the attempt (Python cancels the
  handler; TypeScript aborts `ctx.signal` and cuts the context off from the
  store) and records a retryable `handler_timeout` failure, so backoff,
  `max_attempts` and cancel-wins apply as usual.
- **Retention**: `purge` deletes terminal tasks past a cutoff — nothing else ever
  removes rows, so call it on a schedule. A minimal hourly sweep:

  ```python
  while len(await tasks.purge(older_than_ms=7 * 86_400_000, limit=1_000)) == 1_000:
      pass  # drain in bounded batches; re-run from your scheduler every hour
  ```

- **Operational visibility**: `stats()` returns task counts per queue and status
  (zero-filled), so a dashboard or health check reads backlog without listing
  rows; an `on_error` / `onError` hook on the worker reports what the run loop
  survived (a failed claim, a store write that blew up while finalizing) —
  without it those are silent.

### At-least-once, not exactly-once

A worker can finish an external side effect and then crash before recording
success; after the lease expires the task is redelivered. Make side effects
idempotent using `task_id` or `key`.

### `reuse` is idempotent submit, not "re-run if it failed"

`conflict: "reuse"` returns the task already recorded under that key — *whatever
its state*, including a terminal `failed`/`canceled` one. To force a new run use
`replace` (new task, repoints the key) or `retry` (re-enqueue the same task).

## Storage: SQLite or Postgres

Both backends load the same canonical SQL and pass the same conformance suite, and
everything above the storage seam is identical — the choice is operational, and
switching is one constructor call:

```python
tasks  = CairnQ.sqlite("tasks.db")      # or CairnQ.postgres(dsn)
worker = Worker.sqlite("tasks.db")      # or Worker.postgres(dsn)
```

- **SQLite** — zero setup: the database *is* the runtime. One file, no server, no
  broker, nothing to operate. One host only (see limits below).
- **Postgres** — many hosts, claiming with `FOR UPDATE SKIP LOCKED`, no
  single-writer bottleneck. LISTEN/NOTIFY additionally wakes idle workers and
  `wait`/`call` the moment a task is queued or finishes — millisecond pickup
  instead of the poll interval, with polling kept as the fallback (a lost
  notification costs one poll, never a task). Needs the optional driver:
  `cairnq[postgres]` / `pg`.

One caveat if you target both: Postgres `jsonb` rejects `\u0000` inside strings and
SQLite accepts it, so keep NUL characters out of payloads that need portability.

## Concurrency, limits & when *not* to use it

- **SQLite is same-host only.** WAL needs all processes on one machine and a local
  disk — don't put the file on a network filesystem or share it across hosts.
- **One SQLite writer at a time.** `claim` / `submit` / worker writes are short,
  but heavy write concurrency serializes. Idle workers stay off the write lock (a
  read-only probe gates each poll); busy ones still contend. This is built for
  low-write, long-running AI work — not as a high-throughput message queue.
- **A contended SQLite write waits, but does not block.** Losing the write lock
  costs up to `busyTimeoutMs` (5s default) before the write fails — and the
  process stays responsive throughout, in both SDKs.
- **Nothing is deleted for you.** Terminal tasks accumulate until you call
  `purge` — budget for a retention sweep on a long-lived database.
- **Full trust on the store.** There is no in-database authorization — any process
  that can open it has full access. Protect the file with OS permissions.

## Layout

```
cairnq-protocol/   schema + canonical SQL (per dialect) + conformance scenarios + PROTOCOL.md
cairnq-py/         Python SDK (aiosqlite / asyncpg)
cairnq-node/       TypeScript SDK (better-sqlite3 / pg)
conformance/       cross-language end-to-end orchestrator
```

The protocol is the contract. `cairnq-protocol/sql/<dialect>/*.sql` holds the
**canonical state-transition statements**, loaded verbatim by both SDKs — that
(plus a shared conformance suite both SDKs run against both dialects) is how four
implementations are kept from drifting. Within each SDK, only the dialect layer
differs: every operation is written once above the storage seam. See
[`cairnq-protocol/PROTOCOL.md`](cairnq-protocol/PROTOCOL.md).

## Develop

```bash
# Python SDK + conformance + store/worker tests
cd cairnq-py && uv sync --extra dev && uv run pytest

# TypeScript SDK + conformance + sdk tests
cd cairnq-node && pnpm install && pnpm test

# Cross-language: TS API -> Py worker, and Py API -> TS worker, over one tasks.db
cd conformance && pnpm test:cross-lang

# Micro-benchmarks (same workload in both SDKs; add `postgres` to hit a real PG)
cd cairnq-node && pnpm bench
cd cairnq-py && uv run python bench/run.py

# Parameter sweeps: where a drain's time goes, and what concurrency / claim
# batch size / queue count actually cost
cd cairnq-node && pnpm bench:sweep
cd cairnq-py && uv run python bench/sweep.py
```

No database server to start — the runtime is the SQLite file. Set
`CAIRNQ_TEST_PG_DSN` to additionally run both suites against a real Postgres
(CI does this on every push).
