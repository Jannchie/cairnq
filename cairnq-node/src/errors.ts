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

/**
 * How an arbitrary thrown value becomes an envelope. Split out from `asEnvelope`
 * below because the worker also reaches it directly, for a thrown plain object —
 * which `asEnvelope` reads as a ready envelope, the right call for `ctx.fail` and
 * the wrong one for something that was thrown. Both must agree on `code` and on
 * deriving `type` from the error's name, or the same error reads differently
 * depending on which way it was recorded.
 */
export function exceptionEnvelope(err: unknown, retryable = true): Record<string, unknown> {
  const e = err as { name?: string; message?: string };
  return errorEnvelope({
    type: e?.name ?? "Error",
    code: "handler_error",
    message: String(e?.message ?? err),
    retryable,
  });
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

/** A gated submit waited out `maxWaitMs` without the queue draining below its
 * depth limit. Nothing was enqueued. Distinct from a slow submit on purpose: a
 * queue this far behind is a capacity problem, and a caller that silently
 * retries forever converts it into an invisible one. */
export class QueueFull extends CairnQError {
  constructor(
    public queue: string,
    public maxDepth: number,
    public waitedMs: number,
  ) {
    super(
      `queue ${queue} still holds ${maxDepth} or more queued tasks after ` +
        `${waitedMs}ms; refusing to enqueue more`,
    );
    this.name = "QueueFull";
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

/** A value could not be encoded for a protocol JSON column (non-finite number,
 * BigInt, circular structure, …). Raised at the boundary — submit rejects with
 * it, and a worker records a handler result that triggers it as a permanent
 * `unserializable_result` failure. The Python SDK raises the same named error. */
export class SerializationError extends CairnQError {
  constructor(message: string) {
    super(message);
    this.name = "SerializationError";
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

/** What a handler may pass to `ctx.fail`. */
export type FailReason = string | Error | TaskError | Record<string, unknown>;

/**
 * Normalize anything that can end a task into [envelope, retryable].
 *
 * Shared by both ways a failure is recorded — a handler passing a reason to
 * `ctx.fail`, and the worker classifying an error that ended an attempt — so the
 * two cannot disagree about what a given error means. It lives here, beside the
 * envelope constructors it dispatches to, rather than in the module that happens
 * to expose it to handlers.
 *
 * A handler failing one task of a batch has a reason, not an exception object:
 * `item.fail("no source records", { retryable: false })` is the shape the real
 * code wants. A TaskError carries its own retryability and wins over the option;
 * everything else takes the caller's. A ready envelope passes through, which is
 * how the worker hands in the ones it composes itself.
 */
export function asEnvelope(
  error: FailReason,
  retryable: boolean,
): [Record<string, unknown>, boolean] {
  if (error instanceof TaskError) return [error.envelope(), error.retryable];
  if (error instanceof Error) return [exceptionEnvelope(error, retryable), retryable];
  if (typeof error === "object" && error !== null) return [error, retryable];
  // A bare reason is a TaskError in everything but the throwing, so let
  // TaskError own its own type/code defaults rather than restating them.
  return [new TaskError(String(error), { retryable }).envelope(), retryable];
}
