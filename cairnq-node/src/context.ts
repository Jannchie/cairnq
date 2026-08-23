import { DEFAULT_RETRY_BACKOFF_MAX_MS, DEFAULT_RETRY_BACKOFF_MS, failDelayMs } from "./backoff.js";
import { asEnvelope, type FailReason, LostLease } from "./errors.js";
import { cancelRequested, type Task } from "./models.js";
import type { SubmitOptions } from "./client.js";
import type { TaskStore } from "./store/base.js";
import type { PgSession } from "./store/pg-executor.js";
import { type TaskDef, taskName } from "./task.js";
import { pollWait } from "./wait.js";

export interface TaskContextOptions {
  retryBackoffMs?: number;
  retryBackoffMaxMs?: number;
}

/**
 * Handed to a task handler. Worker-side capabilities mirror the Python SDK.
 *
 * One of these per task, whether a handler is delivered one task or a batch: a
 * batch handler receives a `TaskContext[]`, so a single-task handler's `ctx` is
 * literally the batch-of-one element. Lease, cancellation and settlement are per
 * task, which is why they live here rather than on anything batch-shaped.
 */
export class TaskContext {
  private readonly abort = new AbortController();
  private leaseLost = false;
  // Cancellation is monotonic: once the DB has told us a cancel was requested it
  // can't be taken back, so canceled() can answer from this without a re-read.
  private cancelSeen = false;
  // Set once this task reached a terminal state through succeed()/fail(). The
  // worker reads it to know which tasks a batch handler already decided, so it
  // neither settles them twice nor keeps renewing their leases — the bookkeeping
  // every ack/nack-style handler otherwise has to carry itself.
  private isSettled = false;
  private readonly backoffMs: number;
  private readonly backoffMaxMs: number;

  constructor(
    private readonly store: TaskStore,
    private readonly task: Task,
    private readonly ownerId: string,
    private readonly leaseMs: number,
    opts: TaskContextOptions = {},
  ) {
    this.backoffMs = opts.retryBackoffMs ?? DEFAULT_RETRY_BACKOFF_MS;
    this.backoffMaxMs = opts.retryBackoffMaxMs ?? DEFAULT_RETRY_BACKOFF_MAX_MS;
  }

  get taskId(): string {
    return this.task.id;
  }
  /** The worker running this task — what `worker_id` on the row points at. */
  get workerId(): string {
    return this.ownerId;
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
   * True once this task reached a terminal state — whether the handler settled
   * it with succeed()/fail() or the worker settled it on the handler's behalf.
   * The heartbeat and the settlement paths both read it.
   */
  get settled(): boolean {
    return this.isSettled;
  }

  /** @internal Called by the worker when it finalizes this task itself. */
  markSettled(): void {
    this.isSettled = true;
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
    this.observeCancel(cancelRequested(task));
    return task;
  }

  /**
   * @internal The same observation from just the flag, for a caller that read it
   * without materializing a Task — the shared heartbeat, whose statement returns
   * only the id and the cancel column precisely so it does not have to drag
   * every payload back on every beat.
   */
  observeCancel(cancelRequested: boolean): void {
    if (cancelRequested) this.cancelSeen = true;
  }

  /**
   * The half of the gate that asks only "is this attempt still mine?".
   *
   * Checked locally, not just via the store's ownership check — after an
   * abandoned (timed-out) attempt the same worker may re-claim this task under
   * the same workerId, and a zombie handler's write would then pass ownership
   * against the NEW attempt.
   *
   * `owned` layers the settled check on top for writes to this task; `submit`
   * takes this half alone, because a handler may legitimately settle a task and
   * then fan out from it.
   */
  private requireLease(): void {
    if (this.leaseLost) throw new LostLease(this.task.id);
  }

  private async owned(write: () => Promise<Task>): Promise<Task> {
    // One gate for every write through this context, so "may I still write?" is
    // answered in one place rather than at each call site.
    //
    this.requireLease();
    // Settled: the task is terminal, so the statement would match no row and come
    // back as a lost lease — telling the handler "another worker took this" when
    // the truth is "you already finished it", and flipping lostLease on the way.
    // Refuse here instead, without the round trip and without corrupting the
    // lease state.
    if (this.isSettled) throw new LostLease(this.task.id);
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
        workerId: this.ownerId,
        progress: value,
        message,
      }),
    );
  }

  async heartbeat(): Promise<Task> {
    return this.owned(() =>
      this.store.heartbeat({
        taskId: this.task.id,
        workerId: this.ownerId,
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

  // ------------------------------------------------------------- settlement
  // Finalizing a task is normally the worker's job, decided by whether the
  // handler returned or threw. These two let a handler decide one task itself,
  // which is what a batch needs: four of 256 tasks failing for four different
  // reasons is the ordinary case, not the edge one, and it cannot be expressed
  // by a single return value or a single throw.
  //
  // Settling twice is a no-op rather than an error. Handlers built on ack/nack
  // queues all end up carrying a `finalizedIds` set to guarantee exactly that;
  // holding it here instead is the point.

  /**
   * Finalize this task as succeeded, now, without waiting for the handler to
   * return. `complete` semantics: a cancel requested while it ran wins and the
   * task finalizes as canceled instead, its result discarded. Returns null if
   * this task was already settled.
   */
  async succeed(result: unknown = null): Promise<Task | null> {
    if (this.isSettled) return null;
    const task = await this.owned(() =>
      this.store.complete({ taskId: this.task.id, workerId: this.ownerId, result }),
    );
    this.markSettled();
    return task;
  }

  /**
   * Finalize this task as succeeded, committing the caller's own writes in the
   * SAME transaction as the settlement. Whatever `write` returns becomes the
   * task's result.
   *
   *     await ctx.succeedIn(async (session) => {
   *       await db.withSession(session).insert(pages).values(rendered)
   *       return { pages: rendered.length }
   *     })
   *
   * The alternative — write the rows, then settle — has a window between the two
   * commits where the work is durable but the task still reads as running. A
   * crash there re-runs the whole task, which for a render or an ingest means
   * recomputing it, and for non-idempotent work means doing it twice.
   *
   * `session` is the driver's, so this needs a Postgres store built on a
   * PgExecutor the application shares with its own driver; anything else throws.
   * If the settlement finds the lease gone, `write`'s work is rolled back with
   * it and LostLease is raised. Returns null if this task was already settled.
   *
   * `write` may be replayed if the backend retries the transaction — derive
   * nothing inside it that cannot be derived twice.
   */
  async succeedIn<T>(write: (session: PgSession) => Promise<T>): Promise<Task | null> {
    if (this.isSettled) return null;
    const task = await this.owned(async () => {
      const { task } = await this.store.completeIn<PgSession, T>(
        { taskId: this.task.id, workerId: this.ownerId },
        write,
      );
      return task;
    });
    this.markSettled();
    return task;
  }

  /**
   * Finalize this task as failed, now. `error` may be a string reason, an Error,
   * a TaskError (which carries its own retryability), or a ready envelope.
   * Retryable failures get the worker's backoff and are re-queued while attempts
   * remain, exactly as a thrown error would be. Returns null if already settled.
   */
  async fail(
    error: FailReason = "task failed",
    opts: { retryable?: boolean } = {},
  ): Promise<Task | null> {
    if (this.isSettled) return null;
    const [envelope, retryable] = asEnvelope(error, opts.retryable ?? true);
    const task = await this.owned(() =>
      this.store.fail({
        taskId: this.task.id,
        workerId: this.ownerId,
        error: envelope,
        retryable,
        delayMs: failDelayMs(this.task.attempt, retryable, this.backoffMs, this.backoffMaxMs),
      }),
    );
    this.markSettled();
    return task;
  }

  /**
   * Submit a child task; parent/root/correlation are wired automatically.
   *
   * Refused once the lease is gone. Not the full `owned` gate — a handler may
   * legitimately settle a task and then fan out from it, so `settled` is no bar
   * — but a context whose lease was lost is an attempt that has been abandoned:
   * it is being retried elsewhere, and every child it creates now will be
   * created again by that retry. Creating work is the one side effect cairnq can
   * actually stop a zombie handler from repeating.
   */
  submit(name: string, payload?: unknown, opts?: SubmitOptions): Promise<Task>;
  submit<P, R>(task: TaskDef<P, R>, payload?: P, opts?: SubmitOptions): Promise<Task>;
  async submit(task: string | TaskDef, payload?: unknown, opts: SubmitOptions = {}): Promise<Task> {
    this.requireLease();
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
