// STATUSES is the canonical declaration within the TS SDK; TaskStatus derives from
// it so the type and the runtime set can't drift apart. The cross-language source of
// truth is the status CHECK constraint in cairnq-protocol's migration, which the
// conformance suite pins this set against.
export const STATUSES = ["queued", "running", "succeeded", "failed", "canceled"] as const;
export type TaskStatus = (typeof STATUSES)[number];

export interface Task {
  id: string;
  name: string;
  queue: string;
  status: TaskStatus;
  payload: any;
  metadata: any;
  result: any | null;
  error: any | null;
  progress: number | null;
  message: string | null;
  attempt: number;
  max_attempts: number;
  priority: number;
  worker_id: string | null;
  lease_until_ms: number | null;
  run_at_ms: number;
  cancel_requested_at_ms: number | null;
  parent_id: string | null;
  root_id: string | null;
  correlation_id: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  completed_at_ms: number | null;
}

/** The id + status pair the wait loop polls on (see get_status.sql) — a probe,
 * not a snapshot: everything else about the task is deliberately not read. */
export interface TaskRef {
  id: string;
  status: TaskStatus;
}

export const JSON_COLUMNS = ["payload", "result", "error", "metadata"] as const;
// The bigint columns. `attempt` / `max_attempts` / `priority` are int4 and
// `progress` is double precision, so every driver already gives those as numbers;
// only int8 has a wire form worth normalizing. Nullability differs per column
// (completed_at_ms may be null, created_at_ms may not), so the coercion has to
// preserve null rather than turn it into 0.
export const MS_COLUMNS = [
  "lease_until_ms",
  "run_at_ms",
  "cancel_requested_at_ms",
  "created_at_ms",
  "updated_at_ms",
  "completed_at_ms",
] as const;
// As a const tuple so TerminalStatus derives from it — the same declare-once
// pattern as STATUSES/TaskStatus above.
export const TERMINAL = ["succeeded", "failed", "canceled"] as const;
export type TerminalStatus = (typeof TERMINAL)[number];

export function isTerminalStatus(status: TaskStatus): status is TerminalStatus {
  return (TERMINAL as readonly TaskStatus[]).includes(status);
}

/** Map a probe row (see get_status.sql) to a TaskRef — the ref twin of
 * rowToTask, so the row shape stays models' knowledge alone. */
export function rowToRef(row: Record<string, unknown>): TaskRef {
  return { id: row.id as string, status: row.status as TaskStatus };
}

export function rowToTask(row: Record<string, unknown>): Task {
  const t: Record<string, unknown> = { ...row };
  for (const col of JSON_COLUMNS) {
    const v = row[col];
    // The driver decides a JSON column's wire form: SQLite (TEXT) hands back a
    // string to parse; a jsonb-aware driver (Postgres `pg`) hands back an
    // already-decoded object. Parse only a string — never assume one backend.
    t[col] = typeof v === "string" ? JSON.parse(v) : (v ?? null);
  }
  for (const col of MS_COLUMNS) {
    const v = row[col];
    // Same argument, for the other column type the drivers disagree about.
    // Postgres sends int8 down the wire as text to protect precision it cannot
    // know is unneeded; `pg` surfaces that as a string, postgres.js does too,
    // asyncpg decodes to int. Normalizing here rather than demanding it of every
    // driver is what keeps an INJECTED executor honest: the alternative is
    // asking an application to change its driver's global int8 handling to suit
    // cairnq, which breaks that application's own bigint columns.
    //
    // Lossless: every one of these is an epoch-ms, and a millisecond timestamp
    // does not reach Number.MAX_SAFE_INTEGER until the year 287396.
    t[col] = v == null ? null : Number(v);
  }
  return t as unknown as Task;
}

/** Accepts anything carrying a status — a Task or a TaskRef probe. */
export function isTerminal(task: Pick<Task, "status">): boolean {
  return isTerminalStatus(task.status);
}

export function cancelRequested(task: Task): boolean {
  return task.cancel_requested_at_ms != null;
}

// Status predicates — mirror the Python `task.succeeded` properties so callers
// don't compare status strings by hand.
export const isQueued = (task: Task): boolean => task.status === "queued";
export const isRunning = (task: Task): boolean => task.status === "running";
export const isSucceeded = (task: Task): boolean => task.status === "succeeded";
export const isFailed = (task: Task): boolean => task.status === "failed";
export const isCanceled = (task: Task): boolean => task.status === "canceled";
