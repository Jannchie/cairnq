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
function timeoutDetail(task: Task | null, key: string | null): string {
  if (!task) {
    return key === null
      ? "task not found — wrong database file, or already purged?"
      : "no task under this key — never submitted, or already purged?";
  }
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

/** wait/call did not reach a terminal status in time. The task keeps running, so
 * `taskId` is the handle for picking the wait back up — `wait(err.taskId)`
 * re-attaches to the same task from anywhere that can reach the store. `task` is
 * the last snapshot wait() observed (null if the lookup found nothing), and the
 * message says what state it was stuck in — a queued-never-claimed task is the
 * classic first-run failure (no worker, no handler, wrong queue or file).
 *
 * `key` is set when the wait watched a key rather than an id; `taskId` is then
 * the task the key pointed at, or the key itself when it pointed at nothing —
 * there was no id to report. */
export class TaskTimeout extends CairnQError {
  readonly task: Task | null;
  readonly key: string | null;
  constructor(
    public taskId: string,
    opts: { timeoutMs?: number; task?: Task | null; key?: string | null } = {},
  ) {
    const key = opts.key ?? null;
    const subject = key === null ? `task ${taskId}` : `key ${key}`;
    super(
      opts.timeoutMs == null
        ? `${subject} did not finish in time`
        : `${subject} did not finish within ${opts.timeoutMs}ms: ${timeoutDetail(opts.task ?? null, key)}`,
    );
    this.name = "TaskTimeout";
    this.task = opts.task ?? null;
    this.key = key;
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

/**
 * A heartbeat beat came back later than its own interval allowed.
 *
 * The heartbeat shares the event loop with the handlers whose leases it renews,
 * so a handler that blocks the loop stops the renewal with it: the lease expires,
 * the task is recovered and redelivered, and a second worker starts computing
 * what the first is still computing — one task, billed twice, with no error
 * anywhere. Nothing inside the blocked handler can observe that, which is why it
 * is reported through `onError` alongside the other things the run loop survived.
 *
 * The usual cause is synchronous work in a handler: a tight loop, a large
 * JSON.parse, a `*Sync` filesystem or crypto call. Node has one loop and no way
 * to preempt it — move the work to a worker thread, a child process, or an async
 * API that yields. The other cause is a worker simply oversubscribed for its
 * `leaseMs` — nothing is blocking, there is just more work than turns — which the
 * same report covers, because the lease is at equal risk either way.
 */
export class EventLoopBlocked extends CairnQError {
  constructor(
    readonly lateMs: number,
    readonly intervalMs: number,
    readonly leaseMs: number,
  ) {
    super(
      `heartbeat beat was ${lateMs}ms late (interval ${intervalMs}ms, lease ${leaseMs}ms): ` +
        `the event loop was blocked long enough to miss a beat, so this worker's leases ` +
        `are at risk. Usually synchronous work in a handler (move it off the loop); ` +
        `otherwise the worker is oversubscribed for its leaseMs.`,
    );
    this.name = "EventLoopBlocked";
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

/**
 * This store cannot do what was asked, and no argument would change that — the
 * capability belongs to the backend, not to the call.
 *
 * Thrown by `completeIn` on a store with no driver session to share (SQLite has
 * none). A CairnQError rather than a bare Error so the same catch works across
 * both SDKs; the Python SDK raises the same named error.
 */
export class UnsupportedBackend extends CairnQError {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedBackend";
  }
}

/**
 * This connection is not pointed at the cairnq installation the rest of the
 * deployment is using — raised at connect, before any task is written.
 *
 * The schema a Postgres connection resolves to is out-of-band configuration
 * (`search_path`, a `schema` option, an ORM's pool settings), so two processes
 * given the same DSN can still land in different schemas. Every migration is
 * `create table if not exists`, so the odd one out does not fail: it builds a
 * second, empty installation and its protocol version check passes against the
 * cairnq_meta it just created. Left undetected, an API and a worker then agree
 * about everything except WHERE, and no task ever crosses.
 *
 * The Python SDK raises the same named error.
 */
export class SchemaMismatch extends CairnQError {
  constructor(message: string) {
    super(message);
    this.name = "SchemaMismatch";
  }
}

/**
 * The store was closing when this operation asked for it.
 *
 * `close()` waits for the work already accepted — a group commit still holding
 * writes, a transaction with a BEGIN IMMEDIATE open — and turns away everything
 * that arrives after, so that wait cannot be extended indefinitely by a producer
 * that keeps submitting. An operation that lands in that window gets this rather
 * than a driver error about a connection that vanished underneath it.
 *
 * It does not mean the store is finished for good: connecting is lazy, so a
 * store used again after `close()` has returned simply reopens. The Python SDK
 * raises the same named error.
 */
export class StoreClosed extends CairnQError {
  /**
   * Whether this store is gone for good, or merely busy closing.
   *
   * The two need to be told apart without matching the message, because they
   * call for opposite handling: the barrier a close in progress raises clears as
   * soon as that close finishes, so waiting and retrying is right; an in-memory
   * database that has been closed is never coming back, and the same retry is an
   * infinite loop. False (transient) is the default because that is the common
   * case and the safer thing to get wrong.
   */
  constructor(
    message = "store is closing",
    readonly permanent = false,
  ) {
    super(message);
    this.name = "StoreClosed";
  }
}

/** A value could not be encoded for a protocol JSON column (non-finite number,
 * BigInt, circular structure, an opaque built-in like Map or Set, …). Raised at
 * the boundary — submit rejects with it, and a worker records a handler result
 * that triggers it as a permanent `unserializable_result` failure. The Python
 * SDK raises the same named error. */
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
