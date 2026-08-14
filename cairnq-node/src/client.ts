import type { BackpressureOptions } from "./backpressure.js";
import { TaskCanceled, TaskFailed } from "./errors.js";
import { isFailed, isSucceeded, type Task, type TaskStatus } from "./models.js";
import { SQLiteStore } from "./store/sqlite.js";
import { PostgresStore } from "./store/postgres.js";
import type { ListInput, PurgeInput, SubmitInput, TaskStore } from "./store/base.js";
import { type TaskDef, taskName } from "./task.js";
import { pollWait, pollWaitByKey } from "./wait.js";

export type SubmitOptions = Omit<SubmitInput, "name" | "payload">;
export interface CallOptions extends SubmitOptions {
  waitTimeoutMs?: number;
  pollMs?: number;
}

/** Options this handle configures on the store it wraps, rather than the
 * store's own constructor arguments. */
export type ClientOptions = Partial<BackpressureOptions>;

/** API-side handle. Thin wrapper over a TaskStore + SDK-orchestrated wait/call. */
export class CairnQ {
  constructor(
    private readonly _store: TaskStore,
    opts: ClientOptions = {},
  ) {
    // Installed on the store, not held here: every submit path goes through the
    // store, including TaskContext.submit, which this handle never sees.
    if (opts.maxQueueDepth != null) {
      _store.useBackpressure(opts as BackpressureOptions);
    }
  }

  static sqlite(path: string, opts: { busyTimeoutMs?: number } & ClientOptions = {}): CairnQ {
    const { busyTimeoutMs, ...client } = opts;
    return new CairnQ(new SQLiteStore(path, { busyTimeoutMs }), client);
  }

  /** Multi-host backend. `dsn` is a libpq connection string; requires the
   * optional `pg` package. */
  static postgres(dsn: string, opts: { max?: number } & ClientOptions = {}): CairnQ {
    const { max, ...client } = opts;
    return new CairnQ(new PostgresStore(dsn, { max }), client);
  }

  get store(): TaskStore {
    return this._store;
  }

  connect(): Promise<void> {
    return this._store.connect();
  }

  close(): Promise<void> {
    return this._store.close();
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

  /** Wait for a task to finish. Resolves with the terminal Task (any status);
   * throws TaskTimeout without stopping the task, so `wait(err.taskId)` picks the
   * same wait back up — from another process, or after a longer deadline. */
  wait(
    taskId: string,
    opts: { timeoutMs?: number; pollMs?: number } = {},
  ): Promise<Task> {
    return pollWait(this._store, taskId, {
      timeoutMs: opts.timeoutMs ?? 30_000,
      pollMs: opts.pollMs,
    });
  }

  /** Wait for whatever task the `key` currently points at — the cross-process
   * form of picking a wait back up, when the id was never in hand or the process
   * that held it is gone. Re-resolves the key on each poll, so a `replace`
   * landing mid-wait moves the wait onto the new task, and a key with no task
   * yet is waited for rather than rejected. */
  waitByKey(key: string, opts: { timeoutMs?: number; pollMs?: number } = {}): Promise<Task> {
    return pollWaitByKey(this._store, key, {
      timeoutMs: opts.timeoutMs ?? 30_000,
      pollMs: opts.pollMs,
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
    const { waitTimeoutMs = 30_000, pollMs, ...submit } = opts;
    const created = await this.submit(taskName(task), payload, submit);
    const final = await pollWait(this._store, created.id, { timeoutMs: waitTimeoutMs, pollMs });
    if (isSucceeded(final)) return final.result;
    if (isFailed(final)) throw new TaskFailed(final.error);
    throw new TaskCanceled(final.id);
  }
}
