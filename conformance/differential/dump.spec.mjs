/**
 * Tests for the projection itself.
 *
 * The projection's whole risk is that a REAL difference lands in a column it
 * discards. So it is pinned from both sides: a list of mutations that MUST be
 * caught, and a list that must NOT be — the second half is what proves the
 * comparison is not so tight that ordinary non-determinism fails it.
 *
 * When a column is added to COLUMNS.dropped in dump.mjs, a case belongs here
 * saying whether changing it should surface. That turns "what does the
 * projection throw away" from a default buried in code into an assertion.
 */
import { projectRows } from "./dump.mjs";
import { diffDumps } from "./diff.mjs";

let failures = 0;
const check = (name, cond, detail) => {
  if (cond) return;
  failures++;
  console.error(`FAIL ${name}${detail ? `: ${detail}` : ""}`);
};

const BASE_TIME = 1_700_000_000_000;
function row(over = {}) {
  return {
    id: "task_A", name: "job", queue: "default", status: "succeeded",
    payload: '{"n":1}', metadata: '{"dt_key":"t01"}', result: '{"ok":true}', error: null,
    progress: null, message: null, attempt: 1, max_attempts: 3, priority: 0,
    worker_id: "worker_W", lease_until_ms: null, run_at_ms: BASE_TIME,
    cancel_requested_at_ms: null, parent_id: null, root_id: "task_A",
    correlation_id: null, created_at_ms: BASE_TIME, updated_at_ms: BASE_TIME + 5,
    completed_at_ms: BASE_TIME + 5, ...over,
  };
}
const child = (over = {}) =>
  row({ id: "task_B", metadata: '{"dt_key":"t01/c1"}', parent_id: "task_A",
        root_id: "task_A", ...over });

const project = (rows, keys = []) => projectRows(rows, keys);
const drifts = (leftRows, rightRows, keys = [[], []]) =>
  diffDumps(project(leftRows, keys[0]), project(rightRows, keys[1]), { scenario: "spec" });

// ------------------------------------------------------------ must be caught
const MUST_CATCH = [
  ["status", [row()], [row({ status: "failed", completed_at_ms: BASE_TIME + 5 })]],
  ["result", [row()], [row({ result: '{"ok":false}' })]],
  ["result null vs value", [row()], [row({ result: null })]],
  ["attempt", [row()], [row({ attempt: 2 })]],
  ["progress cleared", [row({ progress: 0.5 })], [row()]],
  ["message", [row({ message: "half" })], [row({ message: "done" })]],
  ["error code", [row({ error: '{"code":"a","retryable":false,"type":"E","message":"m"}' })],
                 [row({ error: '{"code":"b","retryable":false,"type":"E","message":"m"}' })]],
  ["error retryable", [row({ error: '{"code":"a","retryable":false,"type":"E","message":"m"}' })],
                      [row({ error: '{"code":"a","retryable":true,"type":"E","message":"m"}' })]],
  ["error present vs absent", [row()], [row({ error: '{"code":"a","retryable":true,"type":"E","message":"m"}' })]],
  ["cancel requested", [row()], [row({ cancel_requested_at_ms: BASE_TIME + 1 })]],
  ["payload", [row()], [row({ payload: '{"n":2}' })]],
  ["metadata (beyond dt_key)", [row({ metadata: '{"dt_key":"t01","tag":"x"}' })],
                               [row({ metadata: '{"dt_key":"t01","tag":"y"}' })]],
  ["queue", [row()], [row({ queue: "gpu" })]],
  ["max_attempts", [row()], [row({ max_attempts: 5 })]],
  ["priority", [row()], [row({ priority: 9 })]],
  ["extra task", [row(), child()], [row()]],
  ["missing task", [row()], [row(), child()]],
  ["parent link", [row(), child()], [row(), child({ parent_id: null })]],
  ["claimed vs never claimed", [row()], [row({ worker_id: null })]],
  ["task key repointed", [row(), child()],
    [row(), child()], [[{ key: "K", task_id: "task_A" }], [{ key: "K", task_id: "task_B" }]]],
];
for (const [name, left, right, keys] of MUST_CATCH) {
  const found = drifts(left, right, keys ?? [[], []]);
  check(`catches ${name}`, found.length > 0, "no drift reported");
}

// -------------------------------------------------------- must NOT be caught
// Legitimate non-determinism. If any of these fails, the projection is too
// tight and every differential run would be a coin flip.
const MUST_IGNORE = [
  ["different ids, same structure",
    [row(), child()],
    [row({ id: "task_Z", root_id: "task_Z" }),
     child({ id: "task_Y", parent_id: "task_Z", root_id: "task_Z" })]],
  ["different worker ids", [row()], [row({ worker_id: "worker_OTHER" })]],
  ["all timestamps shifted",
    [row()],
    [row({ run_at_ms: BASE_TIME + 9e6, created_at_ms: BASE_TIME + 9e6,
           updated_at_ms: BASE_TIME + 9e6 + 5, completed_at_ms: BASE_TIME + 9e6 + 5 })]],
  ["error message text differs",
    [row({ error: '{"code":"a","retryable":true,"type":"E","message":"KeyError: x"}' })],
    [row({ error: '{"code":"a","retryable":true,"type":"E","message":"TypeError: x"}' })]],
  ["json key order differs",
    [row({ payload: '{"a":1,"b":2}' })], [row({ payload: '{"b":2,"a":1}' })]],
  ["retry backoff moved run_at_ms",
    [row({ status: "queued", completed_at_ms: null, run_at_ms: BASE_TIME + 1000 })],
    [row({ status: "queued", completed_at_ms: null, run_at_ms: BASE_TIME + 1900 })]],
];
for (const [name, left, right] of MUST_IGNORE) {
  const found = drifts(left, right);
  check(`ignores ${name}`, found.length === 0, found.join(" | "));
}

// ------------------------------------------------ one-sided invariant checks
const INVALID = [
  ["terminal without completed_at_ms", row({ completed_at_ms: null })],
  ["non-terminal with completed_at_ms", row({ status: "queued", completed_at_ms: BASE_TIME })],
  ["running without a worker", row({ status: "running", worker_id: null, completed_at_ms: null })],
  ["completed before created", row({ completed_at_ms: BASE_TIME - 1 })],
];
for (const [name, bad] of INVALID) {
  const { problems } = project([bad]);
  check(`flags ${name}`, problems.length > 0, "no problem reported");
}
const unnamed = project([row({ metadata: "{}" })]);
check("flags a row with no dt_key", unnamed.problems.some((p) => p.startsWith("UNEXPECTED_ROW")));
const dangling = project([child()]);
check("flags a dangling parent link", dangling.problems.some((p) => p.startsWith("DANGLING_")));

console.log(failures ? `\nPROJECTION SPEC FAILED: ${failures}` : "PROJECTION SPEC OK");
process.exit(failures ? 1 : 0);
