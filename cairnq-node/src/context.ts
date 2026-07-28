import { LostLease } from "./errors.js";
import { cancelRequested, type Task } from "./models.js";
import type { SubmitOptions } from "./client.js";
import type { TaskStore } from "./store/base.js";
import { type TaskDef, taskName } from "./task.js";
import { pollWait } from "./wait.js";

/** Handed to a task handler. Worker-side capabilities mirror the Python SDK. */
export class TaskContext {
  private readonly abort = new AbortController();
  private leaseLost = false;
  // Cancellation is monotonic: once the DB has told us a cancel was requested it
  // can't be taken back, so canceled() can answer from this without a re-read.
  private cancelSeen = false;

  constructor(
    private readonly store: TaskStore,
    private readonly task: Task,
    public readonly workerId: string,
    private readonly leaseMs: number,
  ) {}

  get taskId(): string {
    return this.task.id;
  }
  get name(): string {
    return this.task.name;
  }
  get queue(): string {
    return this.task.queue;
  }
  get attempt(): number {
    return this.task.attempt;
  }
  get metadata(): any {
    return this.task.metadata;
  }
  get rootId(): string | null {
    return this.task.root_id;
  }
  get correlationId(): string | null {
    return this.task.correlation_id;
  }
  get payload(): any {
    return this.task.payload;
  }

  /**
   * True once this worker has lost the task's lease — it expired and another
   * worker reclaimed it. Nothing this handler writes will be recorded any more
   * and the task is already running elsewhere, so a long handler should check
   * this (or `signal`) and bail out instead of continuing to do side effects.
   */
  get lostLease(): boolean {
    return this.leaseLost;
  }

  /** Aborts when the lease is lost. Pass it to fetch / any AbortSignal-aware API. */
  get signal(): AbortSignal {
    return this.abort.signal;
  }

  /** @internal Called by the worker when an owned write reports a lost lease. */
  markLeaseLost(): void {
    if (this.leaseLost) return;
    this.leaseLost = true;
    this.abort.abort(new LostLease(this.task.id));
  }

  // Every owned write returns the current row, so cancellation and lease loss
  // ride along on writes the handler was making anyway.
  private observe(task: Task): Task {
    if (cancelRequested(task)) this.cancelSeen = true;
    return task;
  }

  private async owned(write: () => Promise<Task>): Promise<Task> {
    try {
      return this.observe(await write());
    } catch (err) {
      if (err instanceof LostLease) this.markLeaseLost();
      throw err;
    }
  }

  async progress(value: number | null, message: string | null = null): Promise<Task> {
    return this.owned(() =>
      this.store.progress({
        taskId: this.task.id,
        workerId: this.workerId,
        progress: value,
        message,
      }),
    );
  }

  async heartbeat(): Promise<Task> {
    return this.owned(() =>
      this.store.heartbeat({
        taskId: this.task.id,
        workerId: this.workerId,
        leaseMs: this.leaseMs,
      }),
    );
  }

  /** Cooperative cancel check. Free once a heartbeat has already seen the flag. */
  async canceled(): Promise<boolean> {
    if (this.cancelSeen) return true;
    const t = await this.store.get(this.task.id);
    if (!t) return true;
    if (cancelRequested(t)) this.cancelSeen = true;
    return this.cancelSeen || t.status === "canceled";
  }

  /** Submit a child task; parent/root/correlation are wired automatically. */
  submit(name: string, payload?: unknown, opts?: SubmitOptions): Promise<Task>;
  submit<P, R>(task: TaskDef<P, R>, payload?: P, opts?: SubmitOptions): Promise<Task>;
  async submit(task: string | TaskDef, payload?: unknown, opts: SubmitOptions = {}): Promise<Task> {
    return this.store.submit({
      name: taskName(task),
      payload,
      parentId: this.task.id,
      rootId: this.task.root_id,
      correlationId: this.task.correlation_id,
      ...opts,
    });
  }

  async wait(taskId: string, opts: { timeoutMs?: number; pollMs?: number } = {}): Promise<Task> {
    return pollWait(this.store, taskId, {
      timeoutMs: opts.timeoutMs ?? 30_000,
      pollMs: opts.pollMs,
    });
  }
}
