/**
 * The differential harness: run one workload under each SDK, compare what each
 * left in its database.
 *
 * The conformance scenarios cover the store layer, because their op vocabulary
 * is TaskStore's method list and their inputs are language-neutral JSON. Neither
 * reaches the worker/context/client layer — a JSON scenario cannot express a
 * handler, and cannot even express the INPUT that made the encoder drift (you
 * cannot write a `Map` in JSON). So that layer is compared a different way:
 * both SDKs run the same workload as SUBPROCESSES, and the databases they
 * produce are projected onto behavioural facts and diffed.
 *
 * Subprocesses are not an implementation detail. One of the drifts this exists
 * to catch killed the Node process outright (an unhandled rejection from a
 * worker that failed to start); an in-process runner cannot observe that, and a
 * child's exit code is the whole point.
 *
 * Usage: node conformance/differential/run.mjs [scenario ...]
 */
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { dumpSqlite } from "./dump.mjs";
import { diffDumps, diffWorkers } from "./diff.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..", "..");
const pyDir = join(repoRoot, "cairnq-py");
const nodeDir = join(repoRoot, "cairnq-node");
const env = { ...process.env, CAIRNQ_PROTOCOL_DIR: join(repoRoot, "cairnq-protocol") };

const SIDES = {
  node: ["pnpm", ["--dir", nodeDir, "exec", "tsx", join(here, "node_driver.ts")]],
  py: ["uv", ["run", "--project", pyDir, "python", join(here, "py_driver.py")]],
};

/**
 * Scenarios. `tolerances` widens a single field of a single task to a range,
 * for the paths that genuinely cannot promise an exact value (lease expiry).
 * Declared here, where a reviewer sees it — not inside the projection, where it
 * would quietly widen every comparison.
 */
const SCENARIOS = {
  close_drains_queue: {
    why: "close() must land the writes it already accepted; Node's dropped them.",
    timeoutMs: 60_000,
    tolerances: {},
  },
  background_failure_is_reported: {
    why: "a worker that cannot start must be reported; Node crashed, Python swallowed.",
    timeoutMs: 60_000,
    tolerances: {},
  },
  sweeper_stop_start: {
    why: "a stopped sweeper must still drain on demand, and must be restartable.",
    timeoutMs: 60_000,
    tolerances: {},
  },
  unserializable_result: {
    why: "a result the language cannot encode must fail permanently, not record {}.",
    timeoutMs: 90_000,
    tolerances: {},
  },
};

function runSide(side, scenario, db) {
  const [cmd, baseArgs] = SIDES[side];
  const r = spawnSync(cmd, [...baseArgs, db, scenario], {
    env,
    encoding: "utf-8",
    timeout: SCENARIOS[scenario].timeoutMs,
  });
  // What the orchestrator itself observed about the process. `exited_early` is
  // how a worker that took its host down with it becomes a comparable fact.
  return {
    observed: { exited_early: r.status !== 0, exit_code: r.status ?? null },
    stdout: r.stdout ?? "",
    stderr: r.stderr ?? "",
  };
}

function runScenario(scenario) {
  const dir = mkdtempSync(join(tmpdir(), `cairnq-diff-${scenario}-`));
  const dumps = {};
  const runs = {};
  for (const side of ["node", "py"]) {
    const db = join(dir, `${side}.db`);
    runs[side] = runSide(side, scenario, db);
    try {
      dumps[side] = dumpSqlite(db);
    } catch (err) {
      console.error(`\n${scenario}: could not dump the ${side} database: ${err.message}`);
      console.error(runs[side].stderr.trim());
      return { findings: [`DUMP_FAILED side=${side} scenario=${scenario}`], dir };
    }
    // The projection drops timestamps and error prose; the raw rows keep them,
    // so a failure stays diagnosable past what the comparison chose to compare.
    writeFileSync(join(dir, `${side}.raw.json`), JSON.stringify(dumps[side].raw, null, 2));
    writeFileSync(
      join(dir, `${side}.projected.json`),
      JSON.stringify({ tasks: dumps[side].tasks, task_keys: dumps[side].task_keys }, null, 2),
    );
  }
  const opts = { scenario, tolerances: SCENARIOS[scenario].tolerances };
  return {
    findings: [
      ...diffWorkers(runs.node.observed, runs.py.observed, opts),
      ...diffDumps(dumps.node, dumps.py, opts),
    ],
    dir,
    counts: { node: Object.keys(dumps.node.tasks).length, py: Object.keys(dumps.py.tasks).length },
  };
}

const wanted = process.argv.slice(2);
const names = wanted.length ? wanted : Object.keys(SCENARIOS);
let failed = 0;
for (const scenario of names) {
  if (!SCENARIOS[scenario]) {
    console.error(`unknown scenario: ${scenario}`);
    process.exit(2);
  }
  const { findings, dir, counts } = runScenario(scenario);
  if (findings.length) {
    failed += findings.length;
    console.error(`\n${scenario} — ${SCENARIOS[scenario].why}`);
    for (const f of findings) console.error("  " + f);
    console.error(`  dumps kept for inspection: ${dir}`);
  } else {
    console.log(`${scenario} ok  (${counts.node} tasks both sides)`);
  }
}
if (failed) {
  console.error(`\nDIFFERENTIAL FAILED: ${failed} finding(s)`);
  process.exit(1);
}
console.log(`\nDIFFERENTIAL OK: ${names.length} scenario(s), node and py agree.`);
