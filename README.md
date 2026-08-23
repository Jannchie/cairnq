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
const result = await tasks.call("summarize", { text }, { timeoutMs: 10_000 });
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
if (t?.status === "succeeded") use(t.result);
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
each other. Each name draws its own quota from one claim, so a big `batch` on one
name never starts extra calls for another, and a name with a deep backlog cannot
starve the others.

**A `resource` caps a scarce thing that one or several names contend for** — a
GPU, an index that tolerates a single writer. The limit belongs to the thing, so
it is declared once, on the worker, and the names join it:

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
exclusion across those names. A name that only needs to cap *itself* declares a
resource of its own (`resources={"embed": 2}`, `resource="embed"`). A resource
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
- **Blocking work is handled.** The heartbeat shares the worker's event loop, so
  a handler that occupies it stops renewing its own lease — the task is recovered
  and redelivered, never lost. Python dispatches **sync handlers to a thread**,
  so the usual shape (`def handler(ctx, payload)` around a GPU call) is safe by
  construction; in TypeScript, keep synchronous work off the loop (a worker
  thread, a child process).
- **Retention**: nothing else ever removes rows, so a long-lived database needs a
  sweep — and with payloads that carry real data (an image, a document, a batch of
  embeddings) "nothing removes rows" is a disk leak measured in gigabytes per
  backfill. Give the client a cutoff and it sweeps itself, in bounded batches,
  for as long as the handle is open:

  ```python
  tasks = CairnQ.sqlite("tasks.db", retention_ms=7 * 86_400_000)
  ```

  Tiered retention — a succeeded row is spent once its result is consumed, a
  failed one is worth keeping for diagnosis — is
  `purge(older_than_ms=..., queue=..., status=..., name=..., limit=...)` with
  filters, from your own scheduler or a one-off drain.

- **Operational visibility**: an `on_error` / `onError` hook on the worker
  reports what the run loop survived (a failed claim, a store write that blew up
  while finalizing) — without it those are silent. `queue_depth(queue,
  max_depth)` reads a queue's remaining headroom, bounded so it stays cheap to
  ask on every enqueue.

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
  producer that would rather shed load than wait. The limit is soft across
  several producers (see PROTOCOL.md).

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
  costs up to `busyTimeoutMs` (5s default) before the write fails. Both SDKs spend
  that wait as an awaited backoff rather than inside the driver, so the process
  stays responsive throughout *and* the store keeps serving reads — under WAL a
  reader never needed that lock in the first place.
- **Nothing is deleted for you.** Terminal tasks accumulate until you call
  `purge` — budget for a retention sweep on a long-lived database.
- **Full trust on the store.** There is no in-database authorization — any process
  that can open it has full access. Protect the file with OS permissions.

## Upgrading

Migrations run themselves. The first process to open a database after an upgrade
applies whatever is new, and the check and the apply share one write transaction,
so several processes cold-starting together cannot both decide a migration is
unapplied. There is nothing to run by hand and no separate migration command.

Two things are worth knowing before rolling one out.

**Mixed versions are fine, on purpose.** `protocol_version` is the only thing an
SDK refuses to run against; `schema_version` deliberately is not, so an older SDK
against a newly migrated database keeps working — occasionally slower, never
wrong. That is what makes a rolling upgrade possible: bring the API process and
the workers across in whatever order suits you. (Downgrading is the same
mechanism: migrations are not reversible, and an older SDK simply runs against
the newer schema.)

**A migration that rebuilds an index holds the write lock while it builds.** That
is the one window an upgrade can be felt in, and it grows with the table — which
is another reason to run [retention](#what-you-get) rather than keeping every
terminal row forever. Migration `0008` is the current example, and measured on a
warm local SSD it rebuilds both claim indexes in about **6s for a 1M-row, 500MB
SQLite database** (~2.5s and ~3.4s respectively — the migration records the split;
proportionally less below that, and a cold or networked disk is worse).

During that window every other process's writes wait — and on SQLite they wait
only as long as `busyTimeoutMs` / `busy_timeout_ms` (5s by default) before
failing, so on a database that large the default is *not* enough to sit the
rebuild out. On a big database, then: upgrade in a window where a paused submit
is affordable, or raise the busy timeout for the processes that stay up, or purge
first — a rebuild only pays for the rows still there. On Postgres the same
rebuild blocks writes to `cairnq_tasks` for its duration; there is no
`CONCURRENTLY` available, because it cannot run inside the transaction the
migration ledger needs.

Migration `0009` is a gentler case of the same thing: it only *creates* an index,
and a partial one covering terminal rows only, so it builds over the smaller half
of the table and no older SDK reads it. The lock still has to be held while it
builds, so a large database wants the same window — just a shorter one.

Small databases — the desktop app, the single-host service, anything under a few
hundred thousand rows — need none of this. The rebuild is milliseconds and the
upgrade is invisible.

## Layout

```
cairnq-protocol/   schema + canonical SQL (per dialect) + conformance scenarios + PROTOCOL.md
cairnq-py/         Python SDK (aiosqlite / asyncpg)
cairnq-node/       TypeScript SDK (better-sqlite3 / pg, both optional)
conformance/       cross-language end-to-end orchestrator
```

`cairnq-protocol/surface.json` declares the public API both SDKs must expose, and
the handful of places they deliberately differ, each with a reason. Both sides
check it both ways: everything declared exists here, and everything here is
declared. The conformance suite compares *shared* behavior, so a capability only
one SDK has nothing to disagree with — that gap is what surface.json closes, and
it is enforced rather than aspirational.

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
