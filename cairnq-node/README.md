# cairnq (TypeScript / Node)

SQLite-first, cross-language, storage-centered durable task runtime. The
TypeScript SDK (Node ≥ 20). API and worker processes coordinate only through a
shared SQLite file.

Both drivers are optional peers — install the one you use:
`npm i better-sqlite3` for the SQLite backend, `npm i pg` for Postgres. Importing
`cairnq` loads neither, so a Postgres-only deployment builds no native module.

```ts
import { CairnQ, Worker } from "cairnq";

// Worker side — a handler always receives (ctx, payload).
const worker = Worker.sqlite("tasks.db", { queues: ["gpu"] });
worker.task("image.generate", async (ctx, payload) => {
  await ctx.progress(0.1, "starting");
  return { url: await generate(payload.prompt) };
});
await worker.serve(); // runs until SIGINT/SIGTERM, then closes the store

// API side
const tasks = CairnQ.sqlite("tasks.db");
const task = await tasks.submit("image.generate", { prompt }, {
  key: `user:${userId}:image:${requestId}`,
  queue: "gpu",
  conflict: "reuse",
});
```

Synchronous call (submit + wait):

```ts
import { TaskFailed, TaskTimeout } from "cairnq";

try {
  const result = await tasks.call("summary.create", { text }, { waitTimeoutMs: 10_000 });
} catch (err) {
  if (err instanceof TaskFailed) log(err.code, err.message, err.retryable); // envelope fields
  else if (err instanceof TaskTimeout) {
    // The task keeps running — resume the wait instead of submitting again.
    const result = await tasks.wait(err.taskId, { timeoutMs: 60_000 });
    // …or tasks.waitByKey(key), from a process that never held the id.
  }
}
```

Inspect a task by id/key without matching status strings:

```ts
import { isSucceeded } from "cairnq"; // also isFailed/isCanceled/isRunning/isQueued/isTerminal

const task = await tasks.getByKey(key);
if (task && isSucceeded(task)) use(task.result);
```

Optionally define a task once and share the symbol across both ends — no string
drift, the editor finds every caller, and payload + result are fully typed:

```ts
import { defineTask } from "cairnq";

export const summarize = defineTask<{ text: string }, { summary: string }>("summarize");

worker.task(summarize, async (ctx, payload) => ({ summary: await run(payload.text) }));
const { summary } = await tasks.call(summarize, { text }); // typed result, no cast
```

Opt-in: every API still accepts a plain name string (cross-language callers use it).

## Running it in production

```ts
const worker = Worker.sqlite("tasks.db", {
  concurrency: 4,          // handler calls at once; use maxInFlightBytes to bound memory
  retryBackoffMs: 1_000,   // window doubles per attempt, capped at retryBackoffMaxMs (30s),
                           // jittered over its upper half; 0 disables
  onError: (err, info) => log.warn({ err, ...info }), // claims/writes the loop survived
});

// Nothing else deletes rows, so give the client a retention policy — it sweeps
// terminal tasks in bounded batches for as long as the handle is open. A
// per-status map keeps each status on its own clock (statuses left out are
// never swept): spent results go in minutes, failures stay for diagnosis.
const tasks = CairnQ.sqlite("tasks.db", {
  retention: { olderThanMs: { succeeded: 300_000, failed: 7 * 24 * 3600_000 } },
});
```

A handler that does real side effects should bail out when it loses its lease —
the task is already running on another worker and nothing it writes is recorded:

```ts
worker.task("long.job", async (ctx) => {
  const res = await fetch(url, { signal: ctx.signal }); // aborts on lease loss
  if (ctx.lostLease || (await ctx.canceled())) return;
});
```

## Multi-host

Same code, Postgres instead of the file — `CairnQ.postgres(dsn)` /
`Worker.postgres(dsn)`. Requires the optional `pg` peer dependency (`npm i pg`).

### Sharing the application's connection

Given a `PgExecutor` instead of a DSN, cairnq runs inside a session the
application already has — no second driver, no second pool:

```ts
import { CairnQ, type PgExecutor } from "cairnq";

const executor: PgExecutor = { /* ~30 lines over your driver */ };
const tasks = CairnQ.postgres(executor);
```

`schema` (DSN form only) puts cairnq's tables in a schema of their own:
`CairnQ.postgres(dsn, { schema: "cairnq" })` creates it if absent and sets
`search_path` per connection. The protocol's SQL names no schema, so nothing else
changes. With your own executor, the search_path is yours to set.

Two things an adapter must get right: `int8` has to come back as a JS number
(every cairnq `*_ms` is an epoch or a counter, all inside the safe range), and
`jsonb` as an object rather than a string. An executor cairnq was handed is
never closed by cairnq.

That shared session is also what lets a task's settlement commit together with
the rows the task produced:

```ts
worker.task("render.document", async (ctx, payload) => {
  const rendered = await render(payload);
  return ctx.succeedIn(async (session) => {
    await db.withSession(session).insert(pages).values(rendered);
    return { pages: rendered.length }; // becomes the task's result
  });
});
```

Without it the two are separate transactions, and a crash between them leaves
work durable while the task still reads as running — on retry, recomputed. If
the lease turns out to be gone, the settlement matches no row and the caller's
writes roll back with it.

## Watching

`watch` calls back when the tasks on a queue may have changed — for a dashboard
that would otherwise poll:

```ts
const stop = tasks.watch({ queues: ["render"] }, async (signal) => {
  if (signal.reason === "done") return refreshOne(signal.taskId!);
  setCounts(await tasks.stats());
});
```

It is notify-accelerated polling, not an event log. On Postgres an idle watch
costs nothing and a signal lands within milliseconds; where LISTEN is
unavailable — a transaction-mode pooler, or SQLite, which has no channel — the
timer alone still delivers `poll` signals. So the same consumer is correct either
way, and only its promptness differs. Treat a signal as "re-read now"; the truth
is in `stats()` / `list()` / `get()`.

The protocol (schema + canonical SQL) lives in `../cairnq-protocol` and is shared
verbatim with the Python SDK. See `../cairnq-protocol/PROTOCOL.md`.
