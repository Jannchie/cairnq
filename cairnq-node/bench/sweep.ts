// Parameter sweeps (TypeScript SDK).
//   pnpm bench:sweep            # SQLite on a temp file
//   pnpm bench:sweep postgres   # against CAIRNQ_TEST_PG_DSN (use a throwaway DB)
//
// run.ts answers "how fast is CairnQ". This answers "where does the time go, and
// which knob moves it" — so it reports breakdowns and ratios, not headline
// throughput. Workload sizes and row labels MUST match cairnq-py/bench/sweep.py.
//
//   A  drain breakdown        where a drain's wall time goes, at concurrency 1..64
//   B  finalize batching      what a batched finalize COULD buy, without an API
//   C  claim batch size       what claim(limit=N) already buys
//   D  claim vs queue count   the cost of watching more than one queue
//
// B is the point of the file: it measures the ceiling on a batch-finalize API by
// running today's `complete` statements inside one transaction, so the API only
// gets designed if the ceiling is worth having.
//
// Every number is a median of REPEATS runs on a database freshly created for that
// data point. Both matter: a single run of these varies by ~2x, and a database
// reused across data points carries the churn of the earlier ones into the
// planner's decisions — D is where that showed up as a 3x inflation.
import { rmSync } from "node:fs";
import { dirname } from "node:path";
import { performance } from "node:perf_hooks";

import { CairnQ, defineTask, PostgresStore, SQLiteStore, Worker } from "../src/index.js";
import type { Fetch, Params, TaskStore } from "../src/store/base.js";
import { freshDbPath, waitFor } from "../test/helpers.js";

const REPEATS = 3;
const CONCURRENCIES = [1, 8, 32, 64];
const N_DRAIN = 400;
const N_FINALIZE = 200;
const N_BATCH = 64;
const CLAIM_LIMITS = [1, 8, 64];
const BACKLOGS = [500, 5_000, 20_000];
const N_CLAIM_SAMPLES = 25;
const QUEUE_SETS = [["default"], ["default", "q2"], ["default", "q2", "q3"]];

const noop = defineTask("bench.noop");
const WORKER_ID = "bench-worker";

const pad = (s: string, n: number) => s.padEnd(n);
const ms = (v: number) => `${v.toFixed(1)}ms`;
const us = (v: number) => `${(v * 1000).toFixed(0)}µs`;
const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];

/** Per-statement timings, so a phase breakdown needs no guesswork about which
 * protocol statement a phase maps to. tx() may replay its callback under
 * contention; a retried statement is counted again, which is why `n` is reported
 * next to the totals rather than assumed equal to the workload. */
interface Hit {
  n: number;
  ms: number;
}

/** The wrapper re-exposes the dialect seam, which the class keeps `protected` —
 * B needs to issue today's `complete` inside one transaction of its own. */
type Instrumented = TaskStore & {
  hits: Map<string, Hit>;
  reset(): void;
  tx<T>(fn: (fetch: Fetch) => Promise<T>): Promise<T>;
};

function instrument(store: TaskStore): Instrumented {
  const hits = new Map<string, Hit>();
  const time = async (name: string, run: () => Promise<any[]>) => {
    const t0 = performance.now();
    try {
      return await run();
    } finally {
      const h = hits.get(name) ?? { n: 0, ms: 0 };
      h.n++;
      h.ms += performance.now() - t0;
      hits.set(name, h);
    }
  };
  // The seam is `protected`, so reach it through the prototype rather than by
  // subclassing — this has to wrap SQLiteStore and PostgresStore alike.
  const proto = Object.getPrototypeOf(store) as {
    fetch: (name: string, params: Params) => Promise<any[]>;
    tx: <T>(fn: (fetch: Fetch) => Promise<T>) => Promise<T>;
  };
  const inner = { fetch: proto.fetch.bind(store), tx: proto.tx.bind(store) };
  return Object.assign(store, {
    hits,
    reset: () => hits.clear(),
    fetch: (name: string, params: Params) => time(name, () => inner.fetch(name, params)),
    tx: <T>(fn: (fetch: Fetch) => Promise<T>) =>
      inner.tx((f) => fn((name, params) => time(name, () => f(name, params)))),
  }) as Instrumented;
}

/** One client + one instrumented store on a database nothing else has touched.
 *
 * `analyze` is the backend's own out-of-band ANALYZE. Both stores refresh
 * statistics on their own, but on a minute's interval — a backlog built in
 * seconds would otherwise be planned against the empty table it replaced, which
 * is a different measurement than the one D is after.
 *
 * `seed` bulk-inserts queued rows. Where a backlog is scenery rather than the
 * thing being measured (D), building it through submit() costs one round trip per
 * row — minutes on a networked Postgres, and none of it timed. */
interface Bed {
  tasks: CairnQ;
  store: Instrumented;
  analyze(): Promise<void>;
  seed(n: number, queue: string): Promise<void>;
  close(): Promise<void>;
}

interface Backend {
  name: string;
  bed(): Promise<Bed>;
}

function backendFor(name: string): Backend {
  if (name === "postgres") {
    const dsn = process.env.CAIRNQ_TEST_PG_DSN;
    if (!dsn) throw new Error("set CAIRNQ_TEST_PG_DSN (point it at a throwaway database)");
    return {
      name,
      // One shared database, so "fresh" means emptied and re-analyzed rather than
      // recreated — a stale row estimate would otherwise leak into the next point.
      async bed() {
        const { Pool } = await import("pg");
        const pool = new Pool({ connectionString: dsn, max: 1 });
        const tasks = CairnQ.postgres(dsn);
        const store = instrument(new PostgresStore(dsn));
        await tasks.connect();
        await store.connect();
        // Delete outright rather than purge: purge only sweeps terminal rows, and
        // a queued or running row left by the previous data point would be
        // claimed by the next one's worker and counted in its numbers.
        await pool.query("delete from cairnq_tasks");
        const analyze = async () => void (await pool.query("analyze cairnq_tasks"));
        await analyze();
        return {
          tasks,
          store,
          analyze,
          async seed(n, queue) {
            await pool.query(
              `insert into cairnq_tasks (id, name, queue, status, payload, run_at_ms,
                 created_at_ms, updated_at_ms)
               select 'seed-' || $1 || '-' || i, $2, $1, 'queued', '{}'::jsonb, 0, i, i
               from generate_series(1, $3) as i`,
              [queue, noop.name, n],
            );
          },
          async close() {
            await pool.query("delete from cairnq_tasks");
            await tasks.close();
            await store.close();
            await pool.end();
          },
        };
      },
    };
  }
  return {
    name,
    async bed() {
      const path = freshDbPath();
      const dir = dirname(path);
      const tasks = CairnQ.sqlite(path);
      const store = instrument(new SQLiteStore(path));
      await tasks.connect();
      await store.connect();
      return {
        tasks,
        store,
        // A separate connection, so this cannot be mistaken for something the
        // store does on its own schedule.
        async analyze() {
          const { default: Database } = await import("better-sqlite3");
          const db = new Database(path);
          db.exec("ANALYZE cairnq_tasks");
          db.close();
        },
        async seed(n, queue) {
          const { default: Database } = await import("better-sqlite3");
          const db = new Database(path);
          const ins = db.prepare(
            "insert into cairnq_tasks (id, name, queue, status, payload, run_at_ms," +
              " created_at_ms, updated_at_ms) values (?, ?, ?, 'queued', '{}', 0, ?, ?)",
          );
          db.transaction(() => {
            for (let i = 0; i < n; i++) ins.run(`seed-${queue}-${i}`, noop.name, queue, i, i);
          })();
          db.close();
        },
        async close() {
          await tasks.close();
          await store.close();
          rmSync(dir, { recursive: true, force: true });
        },
      };
    },
  };
}

const submitMany = (tasks: CairnQ, n: number, queue?: string) =>
  Promise.all(Array.from({ length: n }, (_, i) => tasks.submit(noop, { i }, queue ? { queue } : {})));

const finalizeAll = async (tasks: CairnQ, store: Instrumented) => {
  for (const t of await tasks.list({ status: "running", limit: 10_000 })) {
    await store.complete({ taskId: t.id, workerId: WORKER_ID, result: {} });
  }
};

/** Claim exactly `n` tasks, however many round trips that takes. */
async function claimAll(store: Instrumented, n: number, limit = n) {
  const out: string[] = [];
  while (out.length < n) {
    const got = await store.claim({
      queues: ["default"],
      workerId: WORKER_ID,
      leaseMs: 600_000,
      limit: Math.min(limit, n - out.length),
      names: [noop.name],
    });
    if (!got.length) throw new Error("nothing claimable");
    out.push(...got.map((t) => t.id));
  }
  return out;
}

/** Run `fn` on a fresh bed, REPEATS times, and return the median result. */
async function repeat(backend: Backend, fn: (bed: Bed) => Promise<number>) {
  const out: number[] = [];
  for (let i = 0; i < REPEATS; i++) {
    const bed = await backend.bed();
    try {
      out.push(await fn(bed));
    } finally {
      await bed.close();
    }
  }
  return median(out);
}

// ------------------------------------------------------------------- sweep A
// A no-op handler makes the handler term ~0, so whatever wall time remains is the
// runtime's own: claim, finalize, and the idle gaps between polls.
//
// Every statement gets a row rather than a hand-picked few: the worker finalizes
// with `complete`, not `succeed` (cancel-vs-success has to be decided in the same
// write), and a breakdown that assumes otherwise silently reports a zero.
//
// The share-of-wall column is printed only at concurrency 1. Above it the timings
// overlap — a statement's duration includes waiting for the store's in-process
// lock — so the shares would sum past 100% and mean nothing. Read the per-call
// column there instead: it rising with concurrency IS the queueing. Contention-
// free per-op costs come from B and C, which run no worker at all.
async function drainOnce(bed: Bed, conc: number) {
  await submitMany(bed.tasks, N_DRAIN);
  let done = 0;
  let resolveAll!: () => void;
  const allDone = new Promise<void>((r) => (resolveAll = r));
  const w = new Worker(bed.store, ["default"], { concurrency: conc });
  w.task(noop, () => {
    if (++done === N_DRAIN) resolveAll();
    return {};
  });
  bed.store.reset();
  const t0 = performance.now();
  await w.background(async () => {
    await allDone;
    await waitFor(async () => (await bed.tasks.list({ status: "running", limit: 1 })).length === 0);
  });
  return performance.now() - t0;
}

async function drainBreakdown(backend: Backend) {
  console.log(`\nA  drain breakdown — ${N_DRAIN} no-op tasks, median of ${REPEATS}`);
  for (const conc of CONCURRENCIES) {
    // The breakdown comes from the last repeat; the wall time is the median, so a
    // slow outlier cannot dominate the headline number it is quoted next to.
    let hits = new Map<string, Hit>();
    const wall = await repeat(backend, async (bed) => {
      const w = await drainOnce(bed, conc);
      hits = new Map(bed.store.hits);
      return w;
    });
    const serial = conc === 1;
    const share = (v: number) => `${((v / wall) * 100).toFixed(1)}%`;
    const inSql = [...hits.values()].reduce((a, h) => a + h.ms, 0);
    console.log(
      `   conc ${pad(String(conc), 3)}${pad(ms(wall), 10)}${pad(`${Math.round((N_DRAIN / wall) * 1000)} tasks/s`, 14)}` +
        (serial ? `${share(inSql)} inside protocol statements` : ""),
    );
    for (const [name, h] of [...hits].sort((a, b) => b[1].ms - a[1].ms)) {
      console.log(
        `     ${pad(name, 20)}${pad(`n=${h.n}`, 8)}${pad(`${us(h.ms / h.n)}/call`, 12)}` +
          (serial ? `${ms(h.ms)} total  ${share(h.ms)} of wall` : ""),
      );
    }
  }
}

// ------------------------------------------------------------------- sweep B
// The decisive one. Same statements, same rows, same ownership checks — the only
// difference is how many transactions they are spread over.
async function finalizeBatching(backend: Backend) {
  console.log(`\nB  finalize batching — ${N_FINALIZE} completes, median of ${REPEATS}`);
  const serial = await repeat(backend, async (bed) => {
    await submitMany(bed.tasks, N_FINALIZE);
    const ids = await claimAll(bed.store, N_FINALIZE);
    const t0 = performance.now();
    for (const id of ids) {
      await bed.store.complete({ taskId: id, workerId: WORKER_ID, result: {} });
    }
    return performance.now() - t0;
  });
  const batched = await repeat(backend, async (bed) => {
    await submitMany(bed.tasks, N_FINALIZE);
    const ids = await claimAll(bed.store, N_FINALIZE);
    const t0 = performance.now();
    await bed.store.tx(async (f) => {
      for (const id of ids) await f("complete", { id, worker_id: WORKER_ID, result: "{}" });
    });
    return performance.now() - t0;
  });
  console.log(`   ${pad("one txn each", 24)}${pad(ms(serial), 10)}${us(serial / N_FINALIZE)}/task`);
  console.log(`   ${pad("all in one txn", 24)}${pad(ms(batched), 10)}${us(batched / N_FINALIZE)}/task`);
  console.log(`   ${pad("ceiling on a batch API", 24)}${(serial / batched).toFixed(1)}x, on the finalize step alone`);
}

// ------------------------------------------------------------------- sweep C
// The counterpart to B, at the store level: how claim cost scales with limit.
async function claimBatchSize(backend: Backend) {
  console.log(`\nC  claim batch size — moving ${N_BATCH} tasks into 'running', median of ${REPEATS}`);
  for (const limit of CLAIM_LIMITS) {
    const wall = await repeat(backend, async (bed) => {
      await submitMany(bed.tasks, N_BATCH);
      const t0 = performance.now();
      await claimAll(bed.store, N_BATCH, limit);
      const dt = performance.now() - t0;
      await finalizeAll(bed.tasks, bed.store);
      return dt;
    });
    console.log(`   ${pad(`limit=${limit}`, 24)}${pad(ms(wall), 10)}${us(wall / N_BATCH)}/task`);
  }
}

// ------------------------------------------------------------------- sweep D
// Re-derives the numbers PROTOCOL.md quotes, so they can be re-checked after a
// SQLite or driver upgrade rather than trusted indefinitely. The backlog is split
// evenly over the watched queues: a worker watching two queues with work in only
// one is the easy case, and measuring it would understate the cost.
async function claimVsQueueCount(backend: Backend) {
  console.log(`\nD  claim cost vs queue count — median claim of ${N_CLAIM_SAMPLES}, backlog split evenly`);
  console.log(`   ${pad("backlog", 10)}${QUEUE_SETS.map((q) => pad(`${q.length} queue${q.length > 1 ? "s" : ""}`, 12)).join("")}`);
  for (const backlog of BACKLOGS) {
    const cells: string[] = [];
    for (const queues of QUEUE_SETS) {
      const cost = await repeat(backend, async (bed) => {
        for (const q of queues) await bed.seed(Math.round(backlog / queues.length), q);
        await bed.analyze();
        const lat: number[] = [];
        for (let i = 0; i < N_CLAIM_SAMPLES; i++) {
          const t0 = performance.now();
          const rows = await bed.store.claim({
            queues,
            workerId: WORKER_ID,
            leaseMs: 600_000,
            limit: 1,
            names: [noop.name],
          });
          lat.push(performance.now() - t0);
          if (!rows.length) break;
          await bed.store.complete({ taskId: rows[0].id, workerId: WORKER_ID, result: {} });
        }
        return median(lat);
      });
      cells.push(pad(us(cost), 12));
    }
    console.log(`   ${pad(String(backlog), 10)}${cells.join("")}`);
  }
}

async function main() {
  const backend = backendFor(process.argv[2] ?? "sqlite");
  console.log(`cairnq-node sweep  backend=${backend.name}  repeats=${REPEATS}`);
  await drainBreakdown(backend);
  await finalizeBatching(backend);
  await claimBatchSize(backend);
  await claimVsQueueCount(backend);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
