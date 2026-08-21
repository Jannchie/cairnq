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

### Batch delivery (optional)

When the work itself is batched — one embedding call over 256 texts rather than
256 calls — register the name with a batch size and the handler is called once
with a list of contexts:

```python
@worker.task("embed", batch=256)
async def embed(items):                        # list[TaskContext]
    vectors = await model.embed([i.payload["text"] for i in items])
    return {item.task_id: {"vec": v} for item, v in zip(items, vectors)}
```

```ts
worker.task("embed", { batch: 256 }, async (items) => {
  const vectors = await model.embed(items.map((i) => i.payload.text));
  return Object.fromEntries(items.map((i, n) => [i.taskId, { vec: vectors[n] }]));
});
```

One rule: **when the handler returns, every task it did not settle itself is
settled by how the call ended** — returning succeeds them, throwing fails them
retryably, throwing a non-retryable `TaskError` fails them permanently. So the
common cases need no bookkeeping at all.

When a batch ends several ways at once — which is the norm, not the exception —
finalize the odd ones out as you go and let the rest ride on the return:

```python
for item in items:
    if not await source_exists(item.payload["item_id"]):
        await item.fail("no source record", retryable=False)
```

Settling twice is a no-op, so there is no `finalized_ids` set to keep. Everything
else stays per task: each has its own lease, `attempt`, backoff and cancel flag,
and one shared heartbeat renews only the ones still in play.

**`concurrency` counts handler calls, not tasks**, so it is independent of batch
size: a call carrying 256 tasks is one of them, and `batch=256` fills at the
default `concurrency=1`. Size `concurrency` for how much work you want running at
once and `batch` for what the downstream API wants — they no longer trade against
each other, and `maxInFlightBytes` / `max_in_flight_bytes` is what bounds memory.

Cap a single name with its own `concurrency`, so one expensive name cannot take
the whole worker:

```python
@worker.task("embed", batch=256, concurrency=2)   # at most 2 calls at a time
async def embed(items): ...
```

Each name draws its own quota from one claim, so a big `batch` on one name never
starts extra calls for another, and a name with a deep backlog cannot starve the
others.

**A `resource` is that same ceiling shared by several names.** `concurrency` caps
a name against itself, which cannot say what usually binds a worker doing heavy
local work: different handlers contending for one scarce thing — a GPU, an index
that tolerates a single writer. The limit belongs to the thing, so it is declared
once, on the worker, and the names join it:

```python
worker = Worker.sqlite("tasks.db", resources={"gpu": 1, "index": 1})

@worker.task("render", resource="gpu")             # render and compare share one
async def render(ctx, payload): ...                # GPU: at capacity 1, never
                                                   # both at once
@worker.task("compare", resource="gpu")
async def compare(ctx, payload): ...

@worker.task("embed", batch=256, resource="gpu")   # composes with batching
async def embed(items): ...

@worker.task("reindex", resource="index")          # a different scarce thing
async def reindex(ctx, payload): ...
```

```ts
const worker = Worker.sqlite("tasks.db", { resources: { gpu: 1 } });
worker.task("render", { resource: "gpu" }, render);
worker.task("compare", { resource: "gpu" }, compare);
```

Capacity is a count, not a flag, so two GPUs are `{"gpu": 2}`; at 1 it is mutual
exclusion across those names. A name may also cap itself under the shared limit
(`concurrency=1, resource="gpu"`), and the tighter of the two binds. A resource
that no `Worker(resources=...)` declares is rejected at registration rather than
read as unlimited — a typo would otherwise silently remove the ceiling.

The gate sits at claim, which is the point of putting it here rather than
wrapping the handler in a semaphore: a task that has been claimed already holds a
lease, a concurrency slot and a heartbeat, so making it wait its turn *inside* the
handler pays for the exclusion with the very resources the limit exists to
protect. Work blocked on a saturated resource stays `queued` and claimable by
another worker.

**A capacity is per worker process, not per machine or per cluster.** Two replicas
each declaring `{"gpu": 1}` will run two calls against that GPU at once — the
ceiling moved the constraint out of your deployment topology and into the code,
but it did not become distributed. For one GPU, run one worker process (give it
the concurrency it needs for everything else) rather than two replicas that each
believe they own it.

The worker and the API can be in **either language** — a TypeScript API can drive
a Python worker and vice-versa, because the only thing they share is the database
and a JSON protocol.

## What you get

- **`submit / get / wait / call / cancel / retry`**, by `task_id` or business `key`.
- **A wait you can resume.** `wait`/`call` time out without stopping the task, and
  `wait(err.task_id)` — or `wait_by_key(key)` from a process that never held the
  id — picks the same wait back up instead of running the work again.
- **Key conflict strategies**: `reuse` (dedupe work in flight), `reuse-succeeded`
  (also serve a finished result as a cache), `reject`, `replace`.
- **Lease-based claim** with heartbeat and automatic recovery — a crashed worker's
  task is redelivered after its lease expires. A worker only claims tasks it has a
  handler for, so workers with different handler sets share a queue safely — which
  is what makes a Python API and a TypeScript worker on one `default` queue work.
- **At-least-once execution** (honestly — see below). Retries with jittered
  exponential backoff (1s doubling to 30s by default), `attempt` /
  `max_attempts`, queues,
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
- **Blocking work is handled, not merely warned about.** The heartbeat shares the
  worker's event loop, so a handler that occupies it stops renewing its own lease
  — the task is recovered mid-run and a second worker computes it in parallel,
  with no error anywhere. Python dispatches **sync handlers to a thread**, so the
  usual shape (`def handler(ctx, payload)` around a GPU call) is safe by
  construction; when the loop is blocked anyway, both SDKs report
  `EventLoopBlocked` through `on_error` / `onError` while the lease still holds.
- **Retention**: nothing else ever removes rows, so a long-lived database needs a
  sweep — and with payloads that carry real data (an image, a document, a batch of
  embeddings) "nothing removes rows" is a disk leak measured in gigabytes per
  backfill. Give the client a retention policy and it sweeps itself, in bounded
  batches, for as long as the handle is open:

  ```python
  tasks = CairnQ.sqlite("tasks.db", retention=Retention(older_than_ms=7 * 86_400_000))
  ```

  Retention needs are usually tiered — a succeeded row is spent once its result
  is consumed, a failed one is worth keeping for diagnosis — so `older_than_ms`
  also takes a per-status mapping (`{"succeeded": 300_000, "failed":
  86_400_000}`; a status left out is never swept). `purge(older_than_ms=...,
  status=..., name=..., limit=...)` remains the manual form, for an external
  scheduler or a one-off drain.

- **Operational visibility**: `stats()` returns task counts per queue and status
  (zero-filled), so a dashboard or health check reads backlog without listing
  rows; an `on_error` / `onError` hook on the worker reports what the run loop
  survived (a failed claim, a store write that blew up while finalizing) —
  without it those are silent.

- **Backpressure**, so a producer that outruns its workers is bounded by
  something other than disk. Give the client a depth limit and `submit` blocks
  while the queue is at it, then raises `QueueFull` rather than waiting forever:

  ```python
  tasks = CairnQ.postgres(dsn, max_queue_depth={"gpu": 2_000}, max_queue_wait_ms=60_000)
  ```

  The limit is installed on the store, so it holds for every submit — including
  the `ctx.submit` a handler uses to spawn children, which is why `Worker` takes
  the same option (a worker process has no client handle to have set it).

  `queue_depth(queue, max_depth)` is the same read without the blocking, for a
  producer that would rather shed load than wait — bounded at `max_depth` index
  entries, so it stays cheap to ask on every enqueue. The limit is soft across
  several producers (see PROTOCOL.md).

  On the worker side, `max_in_flight_bytes` / `maxInFlightBytes` bounds resident
  payload bytes, which no other option does. `concurrency` counts handler calls,
  and a batched call carries up to `batch` tasks, so the bytes a worker can hold
  are `concurrency * batch * largest-payload` — for payloads carrying media
  inline, the difference between megabytes and gigabytes. It is read between
  claims, so a poll can still overshoot it by one draw per name; size batches with
  that in mind.

### At-least-once, not exactly-once

A worker can finish an external side effect and then crash before recording
success; after the lease expires the task is redelivered. Make side effects
idempotent using `task_id` or `key`.

### `reuse` deduplicates work in flight; reusing a result is opt-in

`conflict: "reuse"` returns the task under that key while it is still
`queued`/`running` — the double-click, the retried request. Once that task
*finishes*, the key is free: the next submit starts a new one. Reusing a
`succeeded` result instead is a cache, correct only when the key encodes the
whole input, so it is spelled out as `conflict: "reuse-succeeded"`. Nothing ever
hands back a `failed` task, which would poison the key until `purge`; use `retry`
to re-run that same task.

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
  `cairnq[postgres]` / `pg` — or, in the TypeScript SDK, no driver of its own at
  all: given a `PgExecutor` over the one the application already runs, cairnq
  joins that session, and `ctx.succeedIn()` then commits a task's settlement in
  the same transaction as the rows the task produced.

Both TypeScript drivers are optional peers (`better-sqlite3`, `pg`) and importing
the package loads neither, so a Postgres-only deployment builds no native module.

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
cairnq-node/       TypeScript SDK (better-sqlite3 / pg, both optional)
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
