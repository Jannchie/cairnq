// CairnQ micro-benchmark (TypeScript SDK).
//   pnpm bench                # SQLite on a temp file
//   pnpm bench postgres       # against CAIRNQ_TEST_PG_DSN (use a throwaway DB)
//
// Workload sizes, poll settings and row labels MUST match cairnq-py/bench/run.py
// — the two benches exist to be compared against each other.
//
// Reports client-op throughput (submit/get/cancel/purge), worker drain
// throughput, and end-to-end call() latency — the latency rows are the ones that
// show the polling floor: with the default intervals a call can't finish faster
// than the worker's claim poll plus wait()'s read poll.
import { rmSync } from "node:fs";
import { dirname } from "node:path";
import { performance } from "node:perf_hooks";

import { CairnQ, defineTask, PostgresStore, SQLiteStore, Worker } from "../src/index.js";
import type { TaskStore } from "../src/index.js";
import { freshDbPath, waitFor } from "../test/helpers.js";

const N_SUBMIT = 1000;
const N_GET = 2000;
const N_DRAIN = 500;
const N_CALL = 40;

const noop = defineTask("bench.noop");
const rows: string[][] = [];

const opsPerSec = (n: number, ms: number) => Math.round((n / ms) * 1000);
const pct = (sorted: number[], q: number) =>
  sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];

async function timed<T>(label: string, n: number, fn: () => Promise<T>): Promise<T> {
  const t0 = performance.now();
  const out = await fn();
  rows.push([label, `${opsPerSec(n, performance.now() - t0)} ops/s`]);
  return out;
}

const pushLatency = (label: string, lat: number[]) =>
  rows.push([label, `p50 ${pct(lat, 0.5).toFixed(0)} ms`, `p95 ${pct(lat, 0.95).toFixed(0)} ms`]);

async function callLatencies(tasks: CairnQ, callOpts: { pollMs?: number } = {}) {
  const lat: number[] = [];
  for (let i = 0; i < N_CALL; i++) {
    const t0 = performance.now();
    await tasks.call(noop, {}, { timeoutMs: 15_000, ...callOpts });
    lat.push(performance.now() - t0);
  }
  return lat.sort((a, b) => a - b);
}

/** Sweep every terminal task, honoring purge's bounded-batch contract. */
async function purgeAll(tasks: CairnQ) {
  while ((await tasks.purge({ olderThanMs: 0, limit: 1_000 })).length === 1_000);
}

/** The drain counter fires on the last handler's return; its complete write —
 * and the handful still in flight — land just after. Wait them out. */
const drainSettled = (tasks: CairnQ) =>
  waitFor(async () => (await tasks.list({ status: "running", limit: 1 })).length === 0);

async function main() {
  const backend = process.argv[2] ?? "sqlite";
  let tasks: CairnQ;
  let workerStore: TaskStore;
  let tmpDir: string | undefined;
  if (backend === "postgres") {
    const dsn = process.env.CAIRNQ_TEST_PG_DSN;
    if (!dsn) throw new Error("set CAIRNQ_TEST_PG_DSN (point it at a throwaway database)");
    tasks = CairnQ.postgres(dsn);
    workerStore = new PostgresStore(dsn);
  } else {
    const path = freshDbPath();
    tmpDir = dirname(path);
    tasks = CairnQ.sqlite(path);
    workerStore = new SQLiteStore(path);
  }
  await tasks.connect();

  // ---- client ops, no worker running. "bench.unclaimed" is a name no worker
  // registers, so these rows sit queued until the cancel pass below.
  const ids = await timed("submit", N_SUBMIT, async () => {
    const out: string[] = [];
    for (let i = 0; i < N_SUBMIT; i++) out.push((await tasks.submit("bench.unclaimed", { i })).id);
    return out;
  });
  const nIds = ids.length;
  await timed("get", N_GET, async () => {
    for (let i = 0; i < N_GET; i++) await tasks.get(ids[i % nIds]);
  });
  await timed("cancel", N_SUBMIT, async () => {
    for (const id of ids) await tasks.cancel(id);
  });
  await timed("purge", N_SUBMIT, () => purgeAll(tasks));

  // ---- drain throughput + call latency at the default poll intervals. The
  // worker store connects up front so one-time setup stays out of the clock;
  // both workers share it (they never run at the same time).
  await Promise.all(Array.from({ length: N_DRAIN }, (_, i) => tasks.submit(noop, { i })));
  let done = 0;
  let resolveAll!: () => void;
  const allDone = new Promise<void>((r) => (resolveAll = r));
  const w = new Worker(workerStore, ["default"], { concurrency: 8 });
  w.task(noop, () => {
    if (++done === N_DRAIN) resolveAll();
    return {};
  });
  await workerStore.connect();
  const t0 = performance.now();
  const [drainMs, lat] = await w.background(async () => {
    await allDone;
    await drainSettled(tasks);
    return [performance.now() - t0, await callLatencies(tasks)] as const;
  });
  rows.push([`drain ${N_DRAIN} (conc 8)`, `${opsPerSec(N_DRAIN, drainMs)} tasks/s`]);
  pushLatency("call e2e (default polls)", lat);

  // ---- call latency with the poll intervals tuned down.
  const w2 = new Worker(workerStore, ["default"], { concurrency: 8, pollIntervalMs: 25 });
  w2.task(noop, () => ({}));
  const lat2 = await w2.background(() => callLatencies(tasks, { pollMs: 10 }));
  pushLatency("call e2e (poll 25ms, wait 10ms)", lat2);

  if (backend === "postgres") await purgeAll(tasks); // shared DB — leave it clean
  await tasks.close();
  await workerStore.close();
  if (tmpDir) rmSync(tmpDir, { recursive: true, force: true });

  console.log(`cairnq-node bench  backend=${backend}`);
  for (const r of rows) console.log("  " + r[0].padEnd(32) + r.slice(1).join("  "));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
