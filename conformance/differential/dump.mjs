/**
 * The normalization projection: a cairnq database as a comparable value.
 *
 * A differential run executes the same workload once with a Node worker and once
 * with a Python worker, then compares the two databases. A raw row-by-row diff
 * would be all noise — ids are random ULIDs, every `*_ms` is a wall clock, and
 * PROTOCOL.md states that within one millisecond the id's random half decides
 * claim order. So the rows are projected onto what is actually a BEHAVIOURAL
 * FACT, and everything else is dropped or reduced to a boolean.
 *
 * Implemented once, here, rather than once per SDK. The point of the whole
 * exercise is to catch drift between two hand-mirrored implementations; a dumper
 * that was itself a twin would be a new drift surface, and two dumpers going
 * wrong the same way would erase the difference they exist to expose.
 */
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Resolved from cairnq-node, which is where better-sqlite3 is installed: this
// file lives outside both packages, so its own directory has no node_modules.
const require = createRequire(
  join(resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "cairnq-node"), "noop.js"),
);

/**
 * Every column of cairnq_tasks, classified. The classification is exhaustive by
 * construction: `project` fails on a column that appears in the table and not
 * here, so a migration adding one cannot be silently ignored — the harness
 * refuses to run until someone decides, in writing, which list it belongs in.
 */
const COLUMNS = {
  /** Kept as-is; a difference here is a difference in behaviour. */
  kept: ["name", "queue", "status", "progress", "message", "attempt", "max_attempts",
         "priority", "correlation_id"],
  /** Kept, but re-serialized with sorted keys so formatting cannot differ. */
  json: ["payload", "result", "metadata"],
  /** Reduced to a boolean: the value is random or a clock, the presence is a fact. */
  boolean: { worker_id: "claimed_ever", lease_until_ms: "lease_held",
             cancel_requested_at_ms: "cancel_requested", completed_at_ms: "completed" },
  /** Rewritten through the id map, so structure survives identity. */
  linked: ["parent_id", "root_id"],
  /** The identity column; becomes the record's key. */
  identity: ["id"],
  /** Projected specially. */
  special: ["error"],
  /** Dropped: pure clocks, checked by the invariants below instead. */
  dropped: ["run_at_ms", "created_at_ms", "updated_at_ms"],
};

/** Stable JSON: object keys sorted at every depth, so key order cannot differ. */
function canonical(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonical);
  const out = {};
  for (const k of Object.keys(value).sort()) out[k] = canonical(value[k]);
  return out;
}

/**
 * An error envelope reduced to its contract.
 *
 * `code` and `retryable` are protocol vocabulary and must match exactly.
 * `message` is explicitly per-SDK diagnostic prose (PROTOCOL.md), so only its
 * presence is compared — the text itself goes to the raw dump, where a human
 * debugging a failure can still read it. `type` is a language class name only
 * when a handler threw something arbitrary, which a scenario declares per task.
 */
function projectError(raw, opaqueType) {
  if (raw == null) return null;
  const e = typeof raw === "string" ? JSON.parse(raw) : raw;
  return {
    code: e.code ?? null,
    retryable: e.retryable ?? null,
    type: opaqueType ? "<opaque>" : (e.type ?? null),
    message_present: typeof e.message === "string" && e.message.length > 0,
    details: canonical(e.details ?? {}),
  };
}

/**
 * Invariants that must hold on ONE side, checked while dumping.
 *
 * Louder than the diff: these catch a database that is wrong even when both
 * sides are wrong the same way, which is exactly the class the shared
 * rowToTask bug belonged to and which a twin-vs-twin comparison is blind to.
 */
function checkInvariants(row, dtKey, problems) {
  const terminal = ["succeeded", "failed", "canceled"].includes(row.status);
  const fail = (msg) => problems.push(`${dtKey}: ${msg}`);
  if (row.created_at_ms > row.updated_at_ms) fail("created_at_ms > updated_at_ms");
  if (terminal && row.completed_at_ms == null) fail(`terminal (${row.status}) but completed_at_ms is null`);
  if (!terminal && row.completed_at_ms != null) fail(`non-terminal (${row.status}) but completed_at_ms is set`);
  if (terminal && row.completed_at_ms < row.created_at_ms) fail("completed before created");
  if (row.attempt < 0) fail(`negative attempt ${row.attempt}`);
  if (row.status === "running" && row.worker_id == null) fail("running with no worker_id");
}

export function projectRows(rows, keyRows, opts = {}) {
  const opaqueTypes = new Set(opts.opaqueErrorTypes ?? []);
  const problems = [];

  // Identity comes from the WORKLOAD, not from the database: the driver stamps
  // metadata.dt_key on every submit, and a handler spawning a child derives the
  // child's key from its own. Numbering rows by their order in the table would
  // wash the id's randomness into the key, since claim order inside one
  // millisecond is decided by exactly that randomness.
  const byId = new Map();
  for (const r of rows) {
    const meta = typeof r.metadata === "string" ? JSON.parse(r.metadata) : (r.metadata ?? {});
    if (typeof meta.dt_key === "string") byId.set(r.id, meta.dt_key);
  }

  const tasks = {};
  for (const r of rows) {
    const dtKey = byId.get(r.id);
    // Loud, not lenient: a row nobody can name means the workload produced
    // something it did not describe, which is a finding, not noise to drop.
    if (dtKey === undefined) { problems.push(`UNEXPECTED_ROW id=${r.id} name=${r.name}`); continue; }
    if (tasks[dtKey]) { problems.push(`DUPLICATE_KEY ${dtKey}`); continue; }
    checkInvariants(r, dtKey, problems);

    const rec = {};
    for (const c of COLUMNS.kept) rec[c] = r[c] ?? null;
    for (const c of COLUMNS.json) {
      const v = r[c];
      const parsed = v == null ? null : typeof v === "string" ? JSON.parse(v) : v;
      rec[c] = canonical(c === "metadata" && parsed ? omit(parsed, "dt_key") : parsed);
    }
    for (const [col, as] of Object.entries(COLUMNS.boolean)) rec[as] = r[col] != null;
    for (const c of COLUMNS.linked) {
      const target = r[c];
      if (target == null) { rec[c.replace("_id", "")] = null; continue; }
      const named = byId.get(target);
      if (named === undefined) problems.push(`DANGLING_${c.toUpperCase()} ${dtKey} -> ${target}`);
      rec[c.replace("_id", "")] = named ?? `<unmapped:${target}>`;
    }
    rec.error = projectError(r.error, opaqueTypes.has(dtKey));
    tasks[dtKey] = rec;
  }

  const taskKeys = {};
  for (const k of keyRows) {
    const named = byId.get(k.task_id);
    if (named === undefined) problems.push(`DANGLING_KEY ${k.key} -> ${k.task_id}`);
    taskKeys[k.key] = named ?? `<unmapped:${k.task_id}>`;
  }

  return { tasks, task_keys: taskKeys, problems };
}

function omit(obj, key) {
  const { [key]: _drop, ...rest } = obj;
  return rest;
}

/** Read a SQLite database and project it. Fails on any column not classified. */
export function dumpSqlite(path, opts = {}) {
  const Database = require("better-sqlite3");
  const db = new Database(path, { readonly: true });
  try {
    const declared = new Set([
      ...COLUMNS.kept, ...COLUMNS.json, ...Object.keys(COLUMNS.boolean),
      ...COLUMNS.linked, ...COLUMNS.identity, ...COLUMNS.special, ...COLUMNS.dropped,
    ]);
    const actual = db.prepare("select * from cairnq_tasks limit 0").columns().map((c) => c.name);
    const unknown = actual.filter((c) => !declared.has(c));
    const missing = [...declared].filter((c) => !actual.includes(c));
    if (unknown.length || missing.length) {
      throw new Error(
        `dump.mjs's column classification is out of date — refusing to run rather than ` +
          `silently ignoring a column. Unknown in table: [${unknown}]. Declared but absent: ` +
          `[${missing}]. Add each new column to COLUMNS in dump.mjs, and add a mutation ` +
          `case to dump.spec.mjs saying whether a change to it must be caught.`,
      );
    }
    const rows = db.prepare("select * from cairnq_tasks").all();
    const keyRows = db.prepare("select key, task_id from cairnq_task_keys").all();
    return { ...projectRows(rows, keyRows, opts), raw: { tasks: rows, task_keys: keyRows } };
  } finally {
    db.close();
  }
}
