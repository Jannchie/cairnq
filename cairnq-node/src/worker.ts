import { TaskContext } from "./context.js";
import { errorEnvelope, LostLease, TaskError } from "./errors.js";
import { newId } from "./ids.js";
import { type Task } from "./models.js";
import { SQLiteStore } from "./store/sqlite.js";
import { PostgresStore } from "./store/postgres.js";
import type { TaskStore } from "./store/base.js";
import { type TaskDef, taskName } from "./task.js";

export type Handler = (ctx: TaskContext, payload: any) => unknown | Promise<unknown>;
/** Handler typed against a TaskDef<P, R>: payload is P, the return is R. */
export type TypedHandler<P, R> = (ctx: TaskContext, payload: P) => R | Promise<R>;

/** Where an error the worker recovered from came from. */
export type ErrorPhase = "claim" | "execute";

export interface WorkerOptions {
  concurrency?: number;
  leaseMs?: number;
  heartbeatIntervalMs?: number;
  pollIntervalMs?: number;
  claimBatch?: number;
  /** Base delay before re-running a failed attempt; doubles per attempt. 0 disables. */
  retryBackoffMs?: number;
  /** Ceiling for the doubling. */
  retryBackoffMaxMs?: number;
  /**
   * Called for errors the worker survived — a claim that threw, a store write
   * that failed while finalizing a task. Without it these are silent: the run
   * loop carries on either way, so this is the only place an operator learns a
   * worker is limping. Must not throw.
   */
  onError?: (err: unknown, info: { phase: ErrorPhase; taskId?: string }) => void;
}

const DEFAULT_RETRY_BACKOFF_MS = 1_000;
const DEFAULT_RETRY_BACKOFF_MAX_MS = 30_000;
/** Wait after a failed claim, so a broken database is not polled in a tight loop. */
const CLAIM_ERROR_BACKOFF_MS = 250;

/** Exponential backoff for the next attempt of a task that just failed. */
export function retryDelayMs(attempt: number, baseMs: number, maxMs: number): number {
  if (baseMs <= 0) return 0;
  const exponent = Math.max(0, attempt - 1);
  return Math.min(maxMs, baseMs * 2 ** exponent);
}

function exceptionEnvelope(err: unknown): Record<string, unknown> {
  const e = err as { name?: string; message?: string };
  return errorEnvelope({
    type: e?.name ?? "Error",
    code: "handler_error",
    message: String(e?.message ?? err),
    retryable: true,
  });
}

export class Worker {
  private readonly handlers = new Map<string, Handler>();
  private readonly workerId = newId("worker");
  private stopped = false;
  private stopResolvers = new Set<() => void>();
  // True only when this worker created its own store (via Worker.sqlite); an
  // injected store may be shared, so serve()/background() must not close it.
  private ownsStore = false;

  constructor(
    private readonly store: TaskStore,
    private readonly queues: string[],
    private readonly opts: WorkerOptions = {},
  ) {}

  static sqlite(
    path: string,
    opts: WorkerOptions & { queues?: string[]; busyTimeoutMs?: number } = {},
  ): Worker {
    const { queues = ["default"], busyTimeoutMs, ...rest } = opts;
    const worker = new Worker(new SQLiteStore(path, { busyTimeoutMs }), queues, rest);
    worker.ownsStore = true;
    return worker;
  }

  /** Multi-host backend. `dsn` is a libpq connection string; requires the
   * optional `pg` package. */
  static postgres(
    dsn: string,
    opts: WorkerOptions & { queues?: string[]; max?: number } = {},
  ): Worker {
    const { queues = ["default"], max, ...rest } = opts;
    const worker = new Worker(new PostgresStore(dsn, { max }), queues, rest);
    worker.ownsStore = true;
    return worker;
  }

  get id(): string {
    return this.workerId;
  }

  task(handler: Handler): this;
  task(name: string, handler: Handler): this;
  task<P, R>(def: TaskDef<P, R>, handler: TypedHandler<P, R>): this;
  task(arg: string | Handler | TaskDef, handler?: Handler): this {
    let name: string;
    let fn: Handler;
    if (typeof arg === "function") {
      // Bare form: worker.task(fn) — registered under the function's name.
      fn = arg;
      name = fn.name;
      if (!name) {
        throw new Error(
          "worker.task(fn): the handler is anonymous; pass a name explicitly, " +
            "e.g. worker.task('summary.create', fn)",
        );
      }
    } else {
      // Named string or a TaskDef — resolve the name the one way everything does.
      name = taskName(arg);
      fn = handler!;
    }
    this.handlers.set(name, fn);
    return this;
  }

  stop(): void {
    this.stopped = true;
    const resolvers = [...this.stopResolvers];
    this.stopResolvers.clear();
    for (const r of resolvers) r();
  }

  /** Close the underlying store connection. Call after run() returns. */
  async close(): Promise<void> {
    await this.store.close();
  }

  // serve()/background() only close a store the worker created itself (via
  // Worker.sqlite). An injected store may be shared with a CairnQ client, so
  // closing it here would pull the connection out from under it.
  private async closeIfOwned(): Promise<void> {
    if (this.ownsStore) await this.close();
  }

  private report(err: unknown, info: { phase: ErrorPhase; taskId?: string }): void {
    try {
      this.opts.onError?.(err, info);
    } catch {
      // A reporting hook must never take the worker down with it.
    }
  }

  async run(opts: { concurrency?: number } = {}): Promise<void> {
    // Clamped: at 0 the loop would await Promise.race([]) — pending forever,
    // beyond even stop()'s reach.
    const concurrency = Math.max(1, opts.concurrency ?? this.opts.concurrency ?? 1);
    const leaseMs = this.opts.leaseMs ?? 30_000;
    const batch = this.opts.claimBatch ?? concurrency;
    await this.store.connect();
    const running = new Set<Promise<void>>();
    try {
      await this.loop(concurrency, batch, leaseMs, running);
    } finally {
      // Whatever ends the loop — stop(), or something unexpected out of the body
      // — nothing this worker started may outlive run(). serve() closes the store
      // as soon as run() settles, and a handler still holding the connection
      // would fault on it.
      await Promise.all([...running]);
    }
  }

  private async loop(
    concurrency: number,
    batch: number,
    leaseMs: number,
    running: Set<Promise<void>>,
  ): Promise<void> {
    const pollMs = this.opts.pollIntervalMs ?? 500;
    while (!this.stopped) {
      const free = concurrency - running.size;
      if (free <= 0) {
        // Wait for a slot rather than spinning. execute() never rejects, so
        // racing these is safe.
        await Promise.race([...running]);
        continue;
      }
      let claimed: Task[];
      try {
        claimed = await this.store.claim({
          queues: this.queues,
          // Only what this worker can run. Queues do not partition work by task
          // name, so another worker's tasks would otherwise be claimed here and
          // failed for want of a handler. Read each poll: handlers may be
          // registered after run() started.
          names: [...this.handlers.keys()],
          workerId: this.workerId,
          leaseMs,
          limit: Math.min(batch, free),
        });
      } catch (err) {
        // A claim can fail transiently (lock contention, a dropped connection).
        // Report it and keep polling — one bad poll must not end the worker.
        this.report(err, { phase: "claim" });
        await this.sleepOrStop(CLAIM_ERROR_BACKOFF_MS);
        continue;
      }
      if (claimed.length === 0) {
        await this.idle(pollMs);
        continue;
      }
      for (const task of claimed) {
        const p = this.execute(task, leaseMs).finally(() => running.delete(p));
        running.add(p);
      }
    }
  }

  /** Blocking-style entry point for a standalone worker process: run until
   * SIGINT/SIGTERM, then close the store. Use this at a script's top level;
   * use run() / background() when you manage the event loop yourself. */
  async serve(opts: { concurrency?: number } = {}): Promise<void> {
    // Signals are installed here rather than in run(): serve() is the entry point
    // that owns the process. run()/background() embed the worker in someone
    // else's process, where a leftover listener suppresses Node's default Ctrl-C
    // handling for the host long after the worker is done.
    const removeSignalHandlers = this.installSignals();
    try {
      await this.run(opts);
    } finally {
      removeSignalHandlers();
      await this.closeIfOwned();
    }
  }

  /** Run the worker in the same process for the duration of fn (deployment mode A). */
  async background<T>(fn: () => Promise<T>, opts: { concurrency?: number } = {}): Promise<T> {
    const runner = this.run(opts);
    try {
      return await fn();
    } finally {
      this.stop();
      await runner;
      await this.closeIfOwned();
    }
  }

  /**
   * Run one task to completion. Never rejects: a task-level failure is reported
   * through onError and the loop moves on. (It used to reject into a promise
   * nobody awaited — an unhandled rejection that took the process down.)
   */
  private async execute(task: Task, leaseMs: number): Promise<void> {
    const ctx = new TaskContext(this.store, task, this.workerId, leaseMs);
    const hb = this.startHeartbeat(ctx, leaseMs);
    try {
      const handler = this.handlers.get(task.name);
      if (!handler) {
        await this.safeFail(
          task,
          errorEnvelope({
            type: "NoHandler",
            code: "no_handler",
            message: `no handler registered for ${task.name}`,
            retryable: false,
          }),
          false,
        );
        return;
      }
      let result: unknown;
      try {
        result = await handler(ctx, task.payload);
      } catch (err) {
        if (err instanceof LostLease) return;
        if (err instanceof TaskError) {
          await this.safeFail(task, err.envelope(), err.retryable);
        } else {
          await this.safeFail(task, exceptionEnvelope(err), true);
        }
        return;
      }
      try {
        // complete (not succeed): finalizes as canceled if a cancel was
        // requested while the handler ran, else succeeded.
        await this.store.complete({ taskId: task.id, workerId: this.workerId, result });
      } catch (err) {
        if (err instanceof LostLease) {
          ctx.markLeaseLost();
          return;
        }
        throw err;
      }
    } catch (err) {
      this.report(err, { phase: "execute", taskId: task.id });
    } finally {
      hb.cancel();
      await hb.done;
    }
  }

  private startHeartbeat(
    ctx: TaskContext,
    leaseMs: number,
  ): { cancel: () => void; done: Promise<void> } {
    let active = true;
    let wake: (() => void) | null = null;
    const interval = this.opts.heartbeatIntervalMs ?? Math.max(1_000, Math.floor(leaseMs / 3));
    const done = (async () => {
      while (active) {
        // Cancellable sleep: cancel() resolves this immediately and clears the
        // timer, so done never hangs on a pending timeout.
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, interval);
          wake = () => {
            clearTimeout(timer);
            resolve();
          };
        });
        wake = null;
        if (!active) break;
        try {
          await ctx.heartbeat();
        } catch (err) {
          // ctx.heartbeat() already flagged the lease as lost for the handler.
          if (err instanceof LostLease) break;
          this.report(err, { phase: "execute", taskId: ctx.taskId });
        }
      }
    })();
    return {
      cancel: () => {
        active = false;
        if (wake) wake();
      },
      done,
    };
  }

  private async safeFail(
    task: Task,
    envelope: Record<string, unknown>,
    retryable: boolean,
  ): Promise<void> {
    const delayMs = retryable
      ? retryDelayMs(
          task.attempt,
          this.opts.retryBackoffMs ?? DEFAULT_RETRY_BACKOFF_MS,
          this.opts.retryBackoffMaxMs ?? DEFAULT_RETRY_BACKOFF_MAX_MS,
        )
      : 0;
    try {
      await this.store.fail({
        taskId: task.id,
        workerId: this.workerId,
        error: envelope,
        retryable,
        delayMs,
      });
    } catch (err) {
      if (!(err instanceof LostLease)) throw err;
    }
  }

  /**
   * The empty-poll sleep. A store with a push channel (Postgres LISTEN/NOTIFY)
   * cuts it short when a task becomes claimable; stop() interrupts it either
   * way, and sleepOrStop bounds it at `ms` so the poll fallback — which also
   * drives lease recovery — never stretches.
   */
  private idle(ms: number): Promise<void> {
    const wake = this.store.claimWake(ms);
    return wake ? Promise.race([this.sleepOrStop(ms), wake]) : this.sleepOrStop(ms);
  }

  private sleepOrStop(ms: number): Promise<void> {
    if (this.stopped) return Promise.resolve();
    return new Promise<void>((resolve) => {
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        // Drop the registration; otherwise every idle poll leaks one closure
        // until stop() is finally called.
        this.stopResolvers.delete(finish);
        resolve();
      };
      const timer = setTimeout(finish, ms);
      this.stopResolvers.add(finish);
    });
  }

  /** Take SIGINT/SIGTERM for the duration of serve(). Returns the undo. */
  private installSignals(): () => void {
    const remove = () => {
      process.off("SIGINT", handler);
      process.off("SIGTERM", handler);
    };
    const handler = () => {
      // Stand down after the first signal, so a second Ctrl-C reaches Node's
      // default and kills a worker that will not drain. `once` would only drop
      // whichever signal fired and leave the other suppressing the default.
      remove();
      this.stop();
    };
    process.on("SIGINT", handler);
    process.on("SIGTERM", handler);
    return remove;
  }
}
