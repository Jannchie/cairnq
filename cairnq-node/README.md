# cairnq (TypeScript / Node)

SQLite-first, cross-language, storage-centered durable task runtime. The
TypeScript SDK (Node ≥ 20, `better-sqlite3`). API and worker processes coordinate
only through a shared SQLite file.

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
    /* err.taskId keeps running */
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
  concurrency: 4,
  retryBackoffMs: 1_000,   // doubles per attempt, capped by retryBackoffMaxMs (30s); 0 disables
  onError: (err, info) => log.warn({ err, ...info }), // claims/writes the loop survived
});

// Nothing else deletes rows. Sweep terminal tasks on a schedule.
await tasks.purge({ olderThanMs: 7 * 24 * 3600_000 });
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

The protocol (schema + canonical SQL) lives in `../cairnq-protocol` and is shared
verbatim with the Python SDK. See `../cairnq-protocol/PROTOCOL.md`.
