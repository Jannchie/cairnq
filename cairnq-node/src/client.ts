import type { BackpressureOptions } from "./backpressure.js";
import { type RetentionOptions, RetentionSweeper } from "./retention.js";
import { TaskCanceled, TaskFailed } from "./errors.js";
import { isFailed, isSucceeded, type Task, type TaskRef, type TaskStatus } from "./models.js";
import { SQLiteStore } from "./store/sqlite.js";
import { PostgresStore } from "./store/postgres.js";
import type { PgExecutor } from "./store/pg-executor.js";
import type {
  ListInput,
  PurgeInput,
  SubmitInput,
  TaskStore,
  WatchOptions,
  WatchSignal,
} from "./store/base.js";
import { type TaskDef, taskName } from "./task.js";
import { DEFAULT_WAIT_TIMEOUT_MS, type PollOptions, pollWait, pollWaitByKey } from "./wait.js";

export type SubmitOptions = Omit<SubmitInput, "name" | "payload">;
/** The wait loop's knobs with the timeout optional (default 30s) — the public
 * face of PollOptions, whose comments document each knob. */
export type WaitOptions = Partial<PollOptions>;
export interface CallOptions extends SubmitOptions, Omit<WaitOptions, "timeoutMs"> {
  waitTimeoutMs?: number;
}

/** Options this handle configures on the store it wraps, rather than the
 * store's own constructor arguments. */
export type ClientOptions = Partial<BackpressureOptions> & {
  /** Delete terminal tasks older than a cutoff, on a schedule, for as long as
   * this handle is open. Off unless set — and off means rows accumulate forever,
   * because nothing else in CairnQ removes them. */
  retention?: RetentionOptions;
};

/** API-side handle. Thin wrapper over a TaskStore + SDK-orchestrated wait/call. */
export class CairnQ {
  /** null unless `retention` was configured. */
  private readonly sweeper: RetentionSweeper | null;

  constructor(
    private readonly _store: TaskStore,
    opts: ClientOptions = {},
  ) {
    // Installed on the store, not held here: every submit path goes through the
    // store, including TaskContext.submit, which this handle never sees.
    if (opts.maxQueueDepth != null) {
      _store.useBackpressure(opts as BackpressureOptions);
    }
    // Retention is the opposite case: it belongs to the handle, because a worker
    // sharing the store must not also be deleting rows behind the API's back.
    // Started here rather than in connect(), which is optional — every other
    // path connects lazily, and retention that silently depends on an optional
    // call is retention that silently does not happen.
    this.sweeper = opts.retention ? new RetentionSweeper(_store, opts.retention) : null;
    this.sweeper?.start();
  }

  static sqlite(path: string, opts: { busyTimeoutMs?: number } & ClientOptions = {}): CairnQ {
    const { busyTimeoutMs, ...client } = opts;
    return new CairnQ(new SQLiteStore(path, { busyTimeoutMs }), client);
  }

  /** Multi-host backend. `source` is a libpq connection string — which requires
   * the optional `pg` package — or a PgExecutor over a driver the application
   * already runs (an ORM's pool, say), which cairnq then shares instead of
   * opening a second one. */
  static postgres(
    source: string | PgExecutor,
    opts: { max?: number; schema?: string } & ClientOptions = {},
  ): CairnQ {
    const { max, schema, ...client } = opts;
    return new CairnQ(new PostgresStore(source, { max, schema }), client);
  }

  get store(): TaskStore {
    return this._store;
  }

  connect(): Promise<void> {
    return this._store.connect();
  }

  /** Stop retention (waiting for a sweep in flight, so no purge outlives the
   * store) and close the store. */
  async close(): Promise<void> {
    await this.sweeper?.stop();
    await this._store.close();
  }

  /** Enqueue a task. With `maxQueueDepth` configured this blocks while the
   * target queue is at its limit, and raises QueueFull if it stays there for
   * `maxQueueWaitMs` — see QueueDepthGate for why that bound is approximate
   * across several producers. */
  submit(name: string, payload?: unknown, opts?: SubmitOptions): Promise<Task>;
  submit<P, R>(task: TaskDef<P, R>, payload?: P, opts?: SubmitOptions): Promise<Task>;
  submit(task: string | TaskDef, payload?: unknown, opts: SubmitOptions = {}): Promise<Task> {
    return this._store.submit({ name: taskName(task), payload, ...opts });
  }

  /** How many more tasks fit on `queue` under `maxDepth` — 0 once it is full.
   * The non-blocking read behind `maxQueueDepth`, for a producer that would
   * rather shed load or pick another queue than wait. Cheaper than `stats()`:
   * bounded at `maxDepth` index entries instead of aggregating the table. */
  queueDepth(queue: string, maxDepth: number): Promise<number> {
    return this._store.queueDepth(queue, maxDepth);
  }

  get(taskId: string): Promise<Task | null> {
    return this._store.get(taskId);
  }

  getByKey(key: string): Promise<Task | null> {
    return this._store.getByKey(key);
  }

  /** The status-only probe wait polls on: id + status, no payload. Public for
   * the same reason it exists — a dashboard or poller that only asks "is it
   * finished yet" should not drag the payload back per ask. */
  getStatus(taskId: string): Promise<TaskRef | null> {
    return this._store.getStatus(taskId);
  }

  getStatusByKey(key: string): Promise<TaskRef | null> {
    return this._store.getStatusByKey(key);
  }

  list(input?: ListInput): Promise<Task[]> {
    return this._store.list(input);
  }

  cancel(taskId: string): Promise<Task | null> {
    return this._store.cancel(taskId);
  }

  cancelByKey(key: string): Promise<Task | null> {
    return this._store.cancelByKey(key);
  }

  retry(taskId: string, opts?: { resetAttempt?: boolean }): Promise<Task | null> {
    return this._store.retry(taskId, opts);
  }

  retryByKey(key: string, opts?: { resetAttempt?: boolean }): Promise<Task | null> {
    return this._store.retryByKey(key, opts);
  }

  /** Delete terminal tasks that finished more than `olderThanMs` ago and return
   * their ids. Nothing else in CairnQ removes rows, so a long-lived database
   * needs this on a schedule. Each call is bounded by `limit` to keep the write
   * short; loop until it returns fewer than `limit`. */
  purge(input?: PurgeInput): Promise<string[]> {
    return this._store.purge(input);
  }

  /** Task counts per queue, keyed by status and zero-filled across all statuses
   * — `(await stats()).default.queued` is the backlog of a queue. */
  stats(): Promise<Record<string, Record<TaskStatus, number>>> {
    return this._store.stats();
  }

  /**
   * Call `onSignal` when the tasks on `queues` may have changed. Returns an
   * unsubscribe.
   *
   * Notify-accelerated polling, not an event log: a signal means "re-read now",
   * and `stats()` / `list()` / `get()` are where the truth is. On Postgres an
   * idle watch costs nothing and signals land in milliseconds; everywhere else
   * the timer alone still delivers, so the same consumer code is correct either
   * way. See TaskStore.watch for the full contract.
   */
  watch(opts: WatchOptions, onSignal: (signal: WatchSignal) => void): () => void {
    return this._store.watch(opts, onSignal);
  }

  /** Wait for a task to finish. Resolves with the terminal Task (any status);
   * throws TaskTimeout without stopping the task, so `wait(err.taskId)` picks the
   * same wait back up — from another process, or after a longer deadline. */
  wait(taskId: string, opts: WaitOptions = {}): Promise<Task> {
    // `??`, not a spread default: a caller forwarding `timeoutMs: undefined`
    // (call() does) must still get the default, and a spread would override it.
    return pollWait(this._store, taskId, {
      ...opts,
      timeoutMs: opts.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS,
    });
  }

  /** Wait for whatever task the `key` currently points at — the cross-process
   * form of picking a wait back up, when the id was never in hand or the process
   * that held it is gone. Re-resolves the key on each poll, so a `replace`
   * landing mid-wait moves the wait onto the new task, and a key with no task
   * yet is waited for rather than rejected. */
  waitByKey(key: string, opts: WaitOptions = {}): Promise<Task> {
    return pollWaitByKey(this._store, key, {
      ...opts,
      timeoutMs: opts.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS,
    });
  }

  /** submit + wait. Resolves with the result on success; rejects with
   * TaskFailed / TaskCanceled / TaskTimeout otherwise. Pass a TaskDef and the
   * resolved value is typed as its Result.
   *
   * `waitTimeoutMs` bounds the wait, not the task: on timeout the task runs on,
   * and `wait(err.taskId)` — or `waitByKey`, from a process that only has the
   * key — resumes the wait rather than starting the work over. */
  async call(name: string, payload?: unknown, opts?: CallOptions): Promise<unknown>;
  async call<P, R>(task: TaskDef<P, R>, payload?: P, opts?: CallOptions): Promise<R>;
  async call(task: string | TaskDef, payload?: unknown, opts: CallOptions = {}): Promise<unknown> {
    const { waitTimeoutMs, pollMs, maxPollMs, ...submit } = opts;
    const created = await this.submit(taskName(task), payload, submit);
    const final = await this.wait(created.id, { timeoutMs: waitTimeoutMs, pollMs, maxPollMs });
    if (isSucceeded(final)) return final.result;
    if (isFailed(final)) throw new TaskFailed(final.error);
    throw new TaskCanceled(final.id);
  }
}
