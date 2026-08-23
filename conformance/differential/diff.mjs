/**
 * Compare two normalized dumps (see dump.mjs) and report every difference as a
 * line naming the task and the path, so a failure says what drifted rather than
 * that something did.
 */

/** Walk two projected values, emitting `path` for each leaf that differs. */
function walk(a, b, path, out) {
  const bothObjects =
    a !== null && b !== null && typeof a === "object" && typeof b === "object" &&
    Array.isArray(a) === Array.isArray(b);
  if (!bothObjects) {
    if (JSON.stringify(a) !== JSON.stringify(b)) out.push({ path, a, b });
    return;
  }
  const keys = [...new Set([...Object.keys(a), ...Object.keys(b)])].sort();
  for (const k of keys) {
    walk(a?.[k] ?? null, b?.[k] ?? null, path ? `${path}.${k}` : k, out);
  }
}

/**
 * `tolerances` lets a scenario declare, per task and field, that only a range is
 * pinned — the lease-expiry paths cannot promise an exact `attempt`. The
 * declaration lives in the scenario file, where a reviewer sees it, rather than
 * inside the projection where it would quietly widen every comparison.
 */
function withinTolerance(dtKey, path, a, b, tolerances) {
  const spec = tolerances?.[dtKey]?.[path];
  if (!spec) return false;
  const inRange = (v) => typeof v === "number" && v >= spec.min && v <= spec.max;
  return inRange(a) && inRange(b);
}

export function diffDumps(left, right, opts = {}) {
  const { leftName = "node", rightName = "py", tolerances = {}, scenario = "?" } = opts;
  const findings = [];
  const add = (task, path, a, b) =>
    findings.push(`DRIFT scenario=${scenario} task=${task} path=${path} ` +
                  `${leftName}=${JSON.stringify(a)} ${rightName}=${JSON.stringify(b)}`);

  // One side's own invariant violations are reported before any comparison:
  // a database that is wrong on its own terms is a stronger signal than two
  // databases that disagree, and the twin-vs-twin diff is blind to a fault both
  // sides share.
  for (const [side, dump] of [[leftName, left], [rightName, right]]) {
    for (const p of dump.problems) findings.push(`INVALID side=${side} scenario=${scenario} ${p}`);
  }

  const keys = [...new Set([...Object.keys(left.tasks), ...Object.keys(right.tasks)])].sort();
  for (const k of keys) {
    const l = left.tasks[k];
    const r = right.tasks[k];
    // A task one side produced and the other did not is the shape a dropped
    // write takes — the group-commit queue that was never drained loses rows.
    if (!l) { findings.push(`MISSING_TASK scenario=${scenario} task=${k} side=${leftName}`); continue; }
    if (!r) { findings.push(`MISSING_TASK scenario=${scenario} task=${k} side=${rightName}`); continue; }
    const leaves = [];
    walk(l, r, "", leaves);
    for (const d of leaves) {
      if (withinTolerance(k, d.path, d.a, d.b, tolerances)) continue;
      add(k, d.path, d.a, d.b);
    }
  }

  const lk = JSON.stringify(left.task_keys);
  const rk = JSON.stringify(right.task_keys);
  if (lk !== rk) add("-", "task_keys", left.task_keys, right.task_keys);

  return findings;
}

/** Compare the orchestrator's own observations of the two worker processes. */
export function diffWorkers(left, right, opts = {}) {
  const { leftName = "node", rightName = "py", scenario = "?" } = opts;
  const out = [];
  walk(left, right, "", out);
  return out.map(
    (d) => `DRIFT scenario=${scenario} task=- path=worker.${d.path} ` +
           `${leftName}=${JSON.stringify(d.a)} ${rightName}=${JSON.stringify(d.b)}`,
  );
}
