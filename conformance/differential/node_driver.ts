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
  background_failure_is_reported: backgroundFailureIsReported,
  unserializable_result: unserializableResult,
};

const run = scenarios[scenario];
if (!run) {
  console.error(`unknown scenario: ${scenario}`);
  process.exit(2);
}
await run();
console.log("DRIVER_DONE");
