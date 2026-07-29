import { nowMs } from "./ids.js";
import { cancelRequested, isQueued, type Task } from "./models.js";

/** The single shape of the JSON error envelope (see PROTOCOL.md). Everything that
 * records an error — a handler exception, a missing handler, lease expiry, a thrown
 * TaskError — builds it here, so the contract's fields live in one place. */
export function errorEnvelope(e: {
  type: string;
  code: string;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
}): Record<string, unknown> {
  return {
    type: e.type,
    code: e.code,
    message: e.message,
    retryable: e.retryable,
    details: e.details ?? {},
  };
}

export class CairnQError extends Error {
  constructor(message?: string) {
    super(message);
    // Subclasses each set their own; without this a bare CairnQError reports
    // "Error", and `err.name` is how callers (and the conformance runner) tell
    // one apart from another.
    this.name = "CairnQError";
  }
}

export class AlreadyExists extends CairnQError {
  constructor(public key: string) {
    super(`task with key ${key} already exists`);
    this.name = "AlreadyExists";
  }
}

/** One line of "why hasn't this finished" from the last snapshot wait()
 * observed. No worker running, no handler for the name, wrong queue, and two
 * processes on different database files all look identical from the API side —
 * queued, never claimed — so that case names the likely causes. */
function timeoutDetail(task: Task | null): string {
  if (!task) return "task not found — wrong database file, or already purged?";
  if (isQueued(task)) {
    const delayMs = task.run_at_ms - nowMs();
    if (task.attempt === 0 && delayMs <= 0) {
      return (
        `never claimed by a worker — is a worker running with a handler for ` +
        `'${task.name}' on queue '${task.queue}', against this same database?`
      );
    }
    const next = delayMs > 0 ? `, next run in ~${delayMs}ms` : "";
    return `still queued (attempt ${task.attempt}/${task.max_attempts})${next}`;
  }
  if (cancelRequested(task)) return "cancel requested, waiting for the handler to observe it";
  return `still running (attempt ${task.attempt}/${task.max_attempts})`;
}

/** wait/call did not reach a terminal status in time. The task keeps running.
 * `task` is the last snapshot wait() observed (null if get() found nothing), and
 * the message says what state it was stuck in — a queued-never-claimed task is
 * the classic first-run failure (no worker, no handler, wrong queue or file). */
export class TaskTimeout extends CairnQError {
  readonly task: Task | null;
  constructor(
    public taskId: string,
    opts: { timeoutMs?: number; task?: Task | null } = {},
  ) {
    super(
      opts.timeoutMs == null
        ? `task ${taskId} did not finish in time`
        : `task ${taskId} did not finish within ${opts.timeoutMs}ms: ${timeoutDetail(opts.task ?? null)}`,
    );
    this.name = "TaskTimeout";
    this.task = opts.task ?? null;
  }
}

/** A waited-on task ended in `failed`. The envelope's fields are unpacked onto the
 * error — read `e.code` / `e.message` / `e.retryable` / `e.details` instead of
 * digging into `e.error` (the raw envelope stays available on `e.error`). */
export class TaskFailed extends CairnQError {
  readonly type: string;
  readonly code: string;
  readonly retryable: boolean;
  readonly details: Record<string, unknown>;
  constructor(public error: unknown) {
    const env = (error ?? {}) as {
      type?: string;
      code?: string;
      message?: string;
      retryable?: boolean;
      details?: Record<string, unknown>;
    };
    super(env.message ?? "task failed");
    this.name = "TaskFailed";
    this.type = env.type ?? "TaskError";
    this.code = env.code ?? "task_error";
    this.retryable = env.retryable ?? false;
    this.details = env.details ?? {};
  }
}

export class TaskCanceled extends CairnQError {
  constructor(public taskId: string) {
    super(`task ${taskId} was canceled`);
    this.name = "TaskCanceled";
  }
}

/** A worker write affected 0 rows: the lease expired and was reclaimed. */
export class LostLease extends CairnQError {
  constructor(public taskId: string) {
    super(`lost lease on task ${taskId}`);
    this.name = "LostLease";
  }
}

export class ProtocolVersionMismatch extends CairnQError {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolVersionMismatch";
  }
}

/** Throw inside a handler to control how the failure is recorded. Defaults to
 * non-retryable so deterministic errors fail fast instead of burning retries.
 * Any other thrown value is treated as retryable. */
export class TaskError extends CairnQError {
  code: string;
  retryable: boolean;
  type: string;
  details: Record<string, unknown>;
  constructor(
    message: string,
    opts: { code?: string; retryable?: boolean; type?: string; details?: Record<string, unknown> } = {},
  ) {
    super(message);
    this.name = "TaskError";
    this.code = opts.code ?? "task_error";
    this.retryable = opts.retryable ?? false;
    this.type = opts.type ?? "TaskError";
    this.details = opts.details ?? {};
  }
  envelope(): Record<string, unknown> {
    return errorEnvelope({
      type: this.type,
      code: this.code,
      message: this.message,
      retryable: this.retryable,
      details: this.details,
    });
  }
}
