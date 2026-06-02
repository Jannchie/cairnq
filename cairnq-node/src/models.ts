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

const JSON_COLUMNS = ["payload", "result", "error", "metadata"] as const;
export const TERMINAL: TaskStatus[] = ["succeeded", "failed", "canceled"];

export function rowToTask(row: Record<string, unknown>): Task {
  const t: Record<string, unknown> = { ...row };
  for (const col of JSON_COLUMNS) {
    const v = row[col];
    // The driver decides a JSON column's wire form: SQLite (TEXT) hands back a
    // string to parse; a jsonb-aware driver (Postgres `pg`) hands back an
    // already-decoded object. Parse only a string — never assume one backend.
    t[col] = typeof v === "string" ? JSON.parse(v) : (v ?? null);
  }
  return t as unknown as Task;
}

export function isTerminal(task: Task): boolean {
  return TERMINAL.includes(task.status);
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
