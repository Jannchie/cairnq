// One side of a differential run: performs a scenario's workload against `db`
// using the TypeScript SDK, then exits. The Python driver beside this one must
// do the SAME work through the same public API — the point is to compare what
// the two SDKs leave in the database, so any difference in the driver itself is
// a difference the comparison would wrongly blame on the SDK.
//
// Every submit stamps `metadata.dt_key`: identity in the dump comes from the
// workload, not from the random ULIDs. See dump.mjs.
import { CairnQ, Worker } from "../../cairnq-node/src/index.js";
import { join } from "node:path";

const [, , db, scenario] = process.argv;

/**
 * Writes accepted but not awaited, then an immediate close.
 *
 * SQLiteStore batches writes already waiting on its lock into one transaction,
 * so at the moment close() is called a group commit is holding submits whose
 * callers are still awaiting them. A close that does not drain loses exactly
 * those rows — and the count that comes back is the only evidence.
 */
async function closeDrainsQueue(): Promise<void> {
  const tasks = CairnQ.sqlite(db);
  await tasks.connect();
  const inFlight = Array.from({ length: 64 }, (_, i) =>
    tasks.submit("job", { i }, { metadata: { dt_key: `t${String(i).padStart(2, "0")}` } }),
  );
  await tasks.close();
  await Promise.allSettled(inFlight);
}

/**
 * A worker that cannot start, and whether the SDK says so.
 *
 * The outcome is written INTO the database as a task, because that is the only
 * channel the two sides' observations can be compared through. Node used to let
 * run()'s rejection go unattached and its default handling killed the process,
 * so `outcome` was never submitted at all — a MISSING_TASK. Python suppressed
 * the failure, so it recorded reported=false. Both now record reported=true.
 */
async function backgroundFailureIsReported(): Promise<void> {
  const tasks = CairnQ.sqlite(db);
  await tasks.connect();
  await tasks.submit("marker", { at: "start" }, { metadata: { dt_key: "t00" } });

  // A path no process can open, so connect() fails inside run().
  const worker = Worker.sqlite(join("/proc/cairnq-cannot-exist", "tasks.db"));
  worker.task("noop", async () => null);
  let reported = false;
  let bodyRan = false;
  try {
    await worker.background(async () => {
      await new Promise((r) => setTimeout(r, 50));
      bodyRan = true;
    });
  } catch {
    reported = true;
  }

  await tasks.submit("outcome", { reported, bodyRan }, { metadata: { dt_key: "t01" } });
  await tasks.close();
}

/**
 * A sweeper stopped and started again, and one drained by hand afterwards.
 *
 * `stopping` is how a sweep in flight cuts itself short, and stop() used to
 * leave it set: a later sweep() returned after its FIRST batch, and in Python
 * a restarted sweeper never swept at all. Both show up as rows that should be
 * gone and are not.
 */
async function sweeperStopStart(): Promise<void> {
  const { RetentionSweeper } = await import("../../cairnq-node/src/retention.js");
  const tasks = CairnQ.sqlite(db);
  await tasks.connect();
  for (let i = 0; i < 7; i++) {
    const t = await tasks.submit("job", { i }, { metadata: { dt_key: `t${String(i).padStart(2, "0")}` } });
    await tasks.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 5_000 });
    await tasks.store.succeed({ taskId: t.id, workerId: "w1", result: { i } });
  }
  // A survivor, so "swept everything" and "swept nothing" are distinguishable.
  await tasks.submit("keep", {}, { metadata: { dt_key: "keep" } });

  const sweeper = new RetentionSweeper(tasks.store, { olderThanMs: 0, limit: 2 });
  sweeper.start();
  await sweeper.stop();
  await sweeper.sweep(); // the on-demand drain a stopped sweeper used to truncate
  await tasks.close();
}

/**
 * A handler returning a value its language cannot put in JSON.
 *
 * Node used to write `Map` as `{}` — succeeded, with the result silently gone.
 * Python's encoder always raised. Both must now record the same permanent
 * `unserializable_result` failure. The offending VALUE is constructed in each
 * language, which is precisely why this cannot be a conformance scenario: JSON
 * has no way to write a Map.
 */
async function unserializableResult(): Promise<void> {
  const tasks = CairnQ.sqlite(db);
  await tasks.connect();
  const worker = Worker.sqlite(db, { pollIntervalMs: 20, retryBackoffMs: 0 });
  worker.task("opaque", async () => new Map([["a", 1]]));
  worker.task("plain", async (_ctx, payload: any) => ({ echoed: payload.i }));

  await tasks.submit("opaque", {}, { metadata: { dt_key: "t00" }, maxAttempts: 1 });
  await tasks.submit("plain", { i: 1 }, { metadata: { dt_key: "t01" } });

  await worker.background(async () => {
    for (let i = 0; i < 200; i++) {
      const all = await tasks.list({});
      if (all.length === 2 && all.every((t) => ["succeeded", "failed", "canceled"].includes(t.status))) break;
      await new Promise((r) => setTimeout(r, 25));
    }
  });
  await tasks.close();
}

const scenarios: Record<string, () => Promise<void>> = {
  close_drains_queue: closeDrainsQueue,
  background_failure_is_reported: backgroundFailureIsReported,
  sweeper_stop_start: sweeperStopStart,
  unserializable_result: unserializableResult,
};

const run = scenarios[scenario];
if (!run) {
  console.error(`unknown scenario: ${scenario}`);
  process.exit(2);
}
await run();
console.log("DRIVER_DONE");
