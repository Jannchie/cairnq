import { TaskCanceled, TaskFailed } from "./errors.js";
import { isFailed, isSucceeded, type Task, type TaskStatus } from "./models.js";
import { SQLiteStore } from "./store/sqlite.js";
import { PostgresStore } from "./store/postgres.js";
import type { ListInput, PurgeInput, SubmitInput, TaskStore } from "./store/base.js";
import { type TaskDef, taskName } from "./task.js";
import { pollWait } from "./wait.js";

export type SubmitOptions = Omit<SubmitInput, "name" | "payload">;
export interface CallOptions extends SubmitOptions {
  waitTimeoutMs?: number;
  pollMs?: number;
}

/** API-side handle. Thin wrapper over a TaskStore + SDK-orchestrated wait/call. */
export class CairnQ {
  constructor(private readonly _store: TaskStore) {}

  static sqlite(path: string, opts?: { busyTimeoutMs?: number }): CairnQ {
    return new CairnQ(new SQLiteStore(path, opts));
  }

  /** Multi-host backend. `dsn` is a libpq connection string; requires the
   * optional `pg` package. */
  static postgres(dsn: string, opts?: { max?: number }): CairnQ {
    return new CairnQ(new PostgresStore(dsn, opts));
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

  submit(name: string, payload?: unknown, opts?: SubmitOptions): Promise<Task>;
  submit<P, R>(task: TaskDef<P, R>, payload?: P, opts?: SubmitOptions): Promise<Task>;
  submit(task: string | TaskDef, payload?: unknown, opts: SubmitOptions = {}): Promise<Task> {
    return this._store.submit({ name: taskName(task), payload, ...opts });
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

  wait(
    taskId: string,
    opts: { timeoutMs?: number; pollMs?: number } = {},
  ): Promise<Task> {
    return pollWait(this._store, taskId, {
      timeoutMs: opts.timeoutMs ?? 30_000,
      pollMs: opts.pollMs,
    });
  }

  /** submit + wait. Resolves with the result on success; rejects with
   * TaskFailed / TaskCanceled / TaskTimeout otherwise. Pass a TaskDef and the
   * resolved value is typed as its Result. */
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
