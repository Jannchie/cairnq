import { DEFAULT_RETRY_BACKOFF_MAX_MS, DEFAULT_RETRY_BACKOFF_MS, failDelayMs } from "./backoff.js";
import { TaskContext } from "./context.js";
import {
  asEnvelope,
  errorEnvelope,
  exceptionEnvelope,
  type FailReason,
  LostLease,
  SerializationError,
} from "./errors.js";
import { newId } from "./ids.js";
import { type Task } from "./models.js";
import { SQLiteStore } from "./store/sqlite.js";
import { PostgresStore } from "./store/postgres.js";
import type { PgExecutor } from "./store/pg-executor.js";
import type { TaskStore } from "./store/base.js";
import { type TaskDef, taskName } from "./task.js";

// Re-exported: they moved to backoff.ts so TaskContext.fail could share them
// without context.ts importing the module that imports it. They stay importable
// from this module, which is where they used to live.
export { retryDelayMs, DEFAULT_RETRY_BACKOFF_MS, DEFAULT_RETRY_BACKOFF_MAX_MS } from "./backoff.js";

export type Handler = (ctx: TaskContext, payload: any) => unknown | Promise<unknown>;
/** Handler typed against a TaskDef<P, R>: payload is P, the return is R. */
export type TypedHandler<P, R> = (ctx: TaskContext, payload: P) => R | Promise<R>;

/**
 * A batch handler takes one argument: the list of contexts. There is no payload
 * shortcut to pair with it — payloads are per task, so they are read off the
 * items (`item.payload`), which is also what a handler must hold to settle one
 * of them.
 *
 * Returning a map of task id -> result fills in results for the tasks the
 * handler did not settle itself; anything else returned is ignored.
 */
export type BatchHandler = (
  items: TaskContext[],
) => void | Record<string, unknown> | Promise<void | Record<string, unknown>>;

/** What `worker.task` recorded for one task name. */
interface Registration {
  fn: Handler | BatchHandler;
  /** Tasks per handler call, or undefined for one-at-a-time delivery. */
  batch?: number;
  /** Resource this name's calls draw from, or undefined to draw from nothing
   * but the worker budget. Declared in `WorkerOptions.resources`. */
  resource?: string;
}

/**
 * One draw's worth of quota: a set of names and how many handler calls they may
 * start. A name that limits itself — by `batch`, or by a `resource` it shares
 * with other names — gets a source to itself, because its quota cannot be
 * expressed in a draw shared with names that count differently. Everything else
 * shares one, where a task is a call and the worker's own budget is the only
 * ceiling.
 */
interface ClaimSource {
  names: string[];
  /** Tasks per call — 1 for the shared source. */
  batch: number;
  /**
   * Resource this source draws from, or undefined. The ceiling it names is
   * shared with the other sources that declare it, which is what keeps two
   * names off one scarce thing at the same time.
   */
  resource?: string;
}

/** One source's share of a poll: the rows it claimed, split into handler calls. */
interface Draw {
  src: ClaimSource;
  calls: [Registration | undefined, Task[]][];
}

/** Where an error the worker recovered from came from. */
export type ErrorPhase = "claim" | "execute";

/**
 * Backpressure is accepted here too, not only on CairnQ: a handler spawning
 * children through TaskContext.submit is a producer, and in a worker process
 * there is usually no CairnQ handle to have configured the store.
 */
export interface WorkerOptions {
  /**
   * Handler calls allowed to run at once. A batch call counts as one, however
   * many tasks it carries — size it for how much work you want in flight, not
   * for how many tasks that comes to.
   */
  concurrency?: number;
  leaseMs?: number;
  pollIntervalMs?: number;
  /** Base delay before re-running a failed attempt; doubles per attempt. 0 disables. */
  retryBackoffMs?: number;
  /** Ceiling for the doubling. */
  retryBackoffMaxMs?: number;
  /**
   * Wall-clock ceiling for one attempt. The heartbeat renews the lease for as
   * long as a handler runs, so a hung handler would otherwise hold its task
   * `running` (and its concurrency slot) forever — cancel can't help,
   * cooperative checks need a live handler. On expiry the worker abandons the
   * attempt (ctx.signal aborts, further ctx writes throw LostLease) and records
   * a retryable `handler_timeout` failure. Unset disables the ceiling.
   */
  maxRunMs?: number;
  /**
   * Call ceilings that several names can draw from, by name — `{ gpu: 1 }`.
   * A handler joins one with `task(name, { resource: "gpu" }, fn)`.
   *
   * The limit belongs to the scarce thing rather than to any one name — a GPU,
   * an index that tolerates a single writer — so it is declared here, once, and
   * at capacity 1 it is mutual exclusion across the names that join it. A name
   * that only needs to cap itself declares a resource of its own.
   */
  resources?: Record<string, number>;
  /** Queued tasks a queue may hold before `submit` blocks — a number for every
   * queue, or a per-queue record that leaves unnamed queues unbounded. */
  maxQueueDepth?: number | Record<string, number>;
  /** How long a blocked submit waits before raising QueueFull. Default 600_000. */
  maxQueueWaitMs?: number;
  /**
   * Called for errors the worker survived — a claim that threw, a store write
   * that failed while finalizing a task. Without it these are silent: the run
   * loop carries on either way, so this is the only place an operator learns a
   * worker is limping. Must not throw.
   */
  onError?: (err: unknown, info: { phase: ErrorPhase; taskId?: string }) => void;
}

/** Wait after a failed claim, so a broken database is not polled in a tight loop. */
const CLAIM_ERROR_BACKOFF_MS = 250;

/** Internal: an attempt outran maxRunMs and was abandoned. */
class AttemptTimeout extends Error {
  constructor(readonly maxRunMs: number) {
    super(`attempt exceeded ${maxRunMs}ms`);
  }
}

function timeoutEnvelope(name: string, maxRunMs: number): Record<string, unknown> {
  return errorEnvelope({
    type: "HandlerTimeout",
    code: "handler_timeout",
    message: `handler for ${name} exceeded maxRunMs=${maxRunMs}ms; the attempt was abandoned`,
    retryable: true,
  });
}

const TIMED_OUT = Symbol("cairnq.timedOut");

/**
 * The worker's claim-scheduling state, in one object: which names draw together
 * and at what batch size, which shared resource each draw spends, how many
 * calls each resource still has room for, and whose turn it is.
 *
 * A name that limits itself — by `batch`, or by a `resource` — needs a quota
 * the shared draw cannot express, so it gets a source of its own; every other
 * name shares one, where a task is a call.
 *
 * A resource is deliberately *not* one source spanning its names: `batch` is
 * per name, and a single source carries one batch size, so two members that
 * batch differently could not share a draw. Keeping a source per name and
 * letting several of them draw down one shared ceiling composes with batching
 * instead of excluding it.
 */
class ClaimPlanner {
  private sources: ClaimSource[] = [];
  /** The union of names the claim probe spans. */
  names: string[] = [];
  /** Calls holding units of each declared resource. */
  private readonly inFlight = new Map<string, number>();
  /** Rotates which source is offered the free budget first — see plan(). */
  private cursor = 0;

  constructor(private readonly capacities: Record<string, number>) {}

  /** Rebuild the sources from the registrations. Called from task(), which is
   * the only place the set changes, so a poll never pays for the rebuild. */
  rebuild(handlers: Map<string, Registration>): void {
    const sources: ClaimSource[] = [];
    const shared: string[] = [];
    for (const [name, reg] of handlers) {
      if (reg.batch != null || reg.resource != null) {
        sources.push({ names: [name], batch: reg.batch ?? 1, resource: reg.resource });
      } else shared.push(name);
    }
    if (shared.length) sources.push({ names: shared, batch: 1 });
    this.sources = sources;
    this.names = [...handlers.keys()];
  }

  get empty(): boolean {
    return this.sources.length === 0;
  }

  /**
   * Draw up to `free` handler calls from one claim transaction.
   *
   * Sources are served in rotating order — without rotating, the first source
   * would take every free slot and the rest would starve behind its backlog.
   * Each draw sizes itself from what the previous ones took, so a source with
   * nothing queued costs nothing; that feedback is why this runs inside the
   * claim transaction rather than dividing the budget up front. It must await
   * nothing but `claim` — the store holds the write lock while it runs.
   *
   * `split` is the worker's deliveries(): rows become handler calls there, so
   * spending the budget against its result is the only way the two cannot
   * disagree.
   */
  async plan(
    free: number,
    claim: (names: string[] | null, limit: number) => Promise<Task[]>,
    split: (rows: Task[]) => [Registration | undefined, Task[]][],
  ): Promise<Draw[]> {
    const cursor = this.cursor % this.sources.length;
    const order = [...this.sources.slice(cursor), ...this.sources.slice(0, cursor)];
    this.cursor = (cursor + 1) % this.sources.length;
    const drawn: Draw[] = [];
    let left = free;
    // Resource units this poll has already drawn. `inFlight` only moves when a
    // call is dispatched, which happens after this whole plan returns — so
    // without a local tally two sources sharing a resource would each see its
    // full ceiling and together overshoot it.
    const taken = new Map<string, number>();
    for (const src of order) {
      if (left <= 0) break;
      const quota = Math.min(this.headroom(src, taken) ?? left, left);
      if (quota <= 0) continue;
      const rows = await claim(src.names, src.batch * quota);
      if (!rows.length) continue;
      const calls = split(rows);
      drawn.push({ src, calls });
      left -= calls.length;
      if (src.resource != null) {
        taken.set(src.resource, (taken.get(src.resource) ?? 0) + calls.length);
      }
    }
    return drawn;
  }

  /** A source's own call ceiling for one poll, or undefined when only the
   * worker-wide budget applies: its resource's capacity less what is running
   * and what earlier draws in this same poll already took. */
  private headroom(src: ClaimSource, taken: Map<string, number>): number | undefined {
    if (src.resource == null) return undefined;
    return Math.max(
      0,
      this.capacities[src.resource] -
        (this.inFlight.get(src.resource) ?? 0) -
        (taken.get(src.resource) ?? 0),
    );
  }

  /** Charge one dispatched call to its resource, for the span of the call. */
  acquire(resource: string | undefined): void {
    if (resource == null) return;
    this.inFlight.set(resource, (this.inFlight.get(resource) ?? 0) + 1);
  }

  /** Give the unit back. Deleting at zero keeps the map to the resources
   * actually in flight, so an idle worker holds no entries at all. */
  release(resource: string | undefined): void {
    if (resource == null) return;
    const rest = (this.inFlight.get(resource) ?? 1) - 1;
    if (rest > 0) this.inFlight.set(resource, rest);
    else this.inFlight.delete(resource);
  }
}

export class Worker {
  private readonly handlers = new Map<string, Registration>();
  private readonly ownId = newId("worker");
  private readonly planner: ClaimPlanner;
  /**
   * Retry backoff, resolved once. Both settlement paths read these — the
   * worker's own `safeFail` and the TaskContext it hands a handler — so
   * resolving the defaults per call site is how the two drift apart.
   */
  private readonly backoffMs: number;
  private readonly backoffMaxMs: number;
  private stopped = false;
  private stopWake!: () => void;
  // Resolved once by stop(); every sleep races against it. A stopped worker
  // never restarts, so one promise serves the instance's lifetime.
  private readonly stopped$ = new Promise<void>((r) => (this.stopWake = r));
  // True only when this worker created its own store (via Worker.sqlite); an
  // injected store may be shared, so serve()/background() must not close it.
  private ownsStore = false;

  constructor(
    private readonly store: TaskStore,
    private readonly queues: string[],
    private readonly opts: WorkerOptions = {},
  ) {
    if (opts.maxRunMs != null && opts.maxRunMs <= 0) {
      throw new Error(`maxRunMs must be > 0, got ${opts.maxRunMs}`);
    }
    for (const [resource, capacity] of Object.entries(opts.resources ?? {})) {
      if (!Number.isInteger(capacity) || capacity < 1) {
        throw new Error(
          `resources[${JSON.stringify(resource)}] must be an integer >= 1, got ${capacity}`,
        );
      }
    }
    if (opts.maxQueueDepth != null) {
      store.useBackpressure({
        maxQueueDepth: opts.maxQueueDepth,
        maxQueueWaitMs: opts.maxQueueWaitMs,
      });
    }
    this.planner = new ClaimPlanner(opts.resources ?? {});
    this.backoffMs = opts.retryBackoffMs ?? DEFAULT_RETRY_BACKOFF_MS;
    this.backoffMaxMs = opts.retryBackoffMaxMs ?? DEFAULT_RETRY_BACKOFF_MAX_MS;
  }

  static sqlite(
    path: string,
    opts: WorkerOptions & { queues?: string[]; busyTimeoutMs?: number } = {},
  ): Worker {
    const { queues = ["default"], busyTimeoutMs, ...rest } = opts;
    const worker = new Worker(new SQLiteStore(path, { busyTimeoutMs }), queues, rest);
    worker.ownsStore = true;
    return worker;
  }

  /** Multi-host backend. `source` is a libpq connection string — which requires
   * the optional `pg` package — or a PgExecutor over a driver the application
   * already runs, which cairnq then shares instead of opening a second pool. */
  static postgres(
    source: string | PgExecutor,
    opts: WorkerOptions & { queues?: string[]; max?: number; schema?: string } = {},
  ): Worker {
    const { queues = ["default"], max, schema, ...rest } = opts;
    const worker = new Worker(new PostgresStore(source, { max, schema }), queues, rest);
    worker.ownsStore = true;
    return worker;
  }

  /** This worker's id — what `worker_id` on a running task points at, and what
   * the Python SDK calls `worker_id` too. */
  get workerId(): string {
    return this.ownId;
  }

  task(handler: Handler): this;
  task(name: string, handler: Handler): this;
  task<P, R>(def: TaskDef<P, R>, handler: TypedHandler<P, R>): this;
  /**
   * Batch delivery: the handler takes one argument, a `TaskContext[]` of up to
   * `batch` tasks, instead of `(ctx, payload)`. Use it when the work itself is
   * batched — one embedding call over 256 texts rather than 256 calls — and size
   * it by what the downstream API wants, not by the queue.
   *
   * `resource` draws each call from a ceiling declared in
   * `WorkerOptions.resources` and shared with every other name that names it —
   * at capacity 1, mutual exclusion across those names.
   */
  task(
    name: string | TaskDef,
    opts: { batch?: number; resource?: string },
    handler: BatchHandler,
  ): this;
  /** A shared resource without batching: the handler still takes `(ctx, payload)`. */
  task(name: string | TaskDef, opts: { resource?: string }, handler: Handler): this;
  task(
    arg: string | Handler | TaskDef,
    second?: Handler | { batch?: number; resource?: string },
    third?: Handler | BatchHandler,
  ): this {
    // Option form: (name | def, { batch?, resource? }, handler). Peel the
    // options off and fall through, so name resolution and registration stay
    // single-sited.
    let batch: number | undefined;
    let resource: string | undefined;
    let handler = second as Handler | BatchHandler | undefined;
    if (second != null && typeof second === "object") {
      if (second.batch != null) {
        if (!Number.isInteger(second.batch) || second.batch < 1) {
          throw new Error(`batch must be an integer >= 1, got ${second.batch}`);
        }
        batch = second.batch;
      }
      resource = second.resource;
      handler = third;
    }
    let name: string;
    let fn: Handler | BatchHandler;
    if (typeof arg === "function") {
      // Bare form: worker.task(fn) — registered under the function's name.
      // Strip the "bound " prefix .bind() stamps on it: otherwise a bound
      // method registers under "bound process", a name no submit ever uses.
      fn = arg;
      name = fn.name.replace(/^(bound )+/, "");
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
    // Loudly, at registration: an undeclared resource would otherwise read as
    // an unbounded one, so a typo would silently remove the ceiling the caller
    // asked for — the failure this option exists to prevent.
    if (resource != null && this.opts.resources?.[resource] == null) {
      const known = Object.keys(this.opts.resources ?? {}).sort().join(", ") || "none";
      throw new Error(
        `task ${JSON.stringify(name)} declares resource ${JSON.stringify(resource)}, ` +
          `which is not in WorkerOptions.resources; declared: ${known}`,
      );
    }
    this.handlers.set(name, { fn, batch, resource });
    this.planner.rebuild(this.handlers);
    return this;
  }

  stop(): void {
    this.stopped = true;
    this.stopWake();
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
    await this.store.connect();
    // The calls in flight — `concurrency` counts these, so the set's size is the
    // budget. How many tasks they carry is the batch sizes' business.
    const running = new Set<Promise<void>>();
    try {
      await this.loop(concurrency, leaseMs, running);
    } finally {
      // Whatever ends the loop — stop(), or something unexpected out of the body
      // — nothing this worker started may outlive run(). serve() closes the store
      // as soon as run() settles, and a handler still holding the connection
      // would fault on it.
      await Promise.all([...running]);
    }
  }

  /**
   * Split one claim into handler calls, each with the registration to run it.
   *
   * A claim is filtered by queue and by the names this worker handles, so it
   * comes back mixed; batch size is per name (one embedding call wants 256
   * texts, one Docling parse wants exactly 1). So group by name, then chunk each
   * group by that name's size. Names registered without `batch` come back as
   * one-task calls, as do names not registered at all — reachable only if a
   * handler is unregistered mid-run, and dispatched to failNoHandler().
   *
   * The registration rides along because this is where it was resolved; looking
   * it up again at the call site would put "is this name batched" in two places.
   */
  private deliveries(claimed: Task[]): [Registration | undefined, Task[]][] {
    const byName = new Map<string, Task[]>();
    for (const task of claimed) {
      const group = byName.get(task.name);
      if (group) group.push(task);
      else byName.set(task.name, [task]);
    }
    const out: [Registration | undefined, Task[]][] = [];
    for (const [name, group] of byName) {
      const reg = this.handlers.get(name);
      const size = reg?.batch ?? 1;
      for (let i = 0; i < group.length; i += size) out.push([reg, group.slice(i, i + size)]);
    }
    return out;
  }

  private context(task: Task, leaseMs: number): TaskContext {
    return new TaskContext(this.store, task, this.ownId, leaseMs, {
      retryBackoffMs: this.backoffMs,
      retryBackoffMaxMs: this.backoffMaxMs,
    });
  }

  private async loop(
    concurrency: number,
    leaseMs: number,
    running: Set<Promise<void>>,
  ): Promise<void> {
    const pollMs = this.opts.pollIntervalMs ?? 500;
    while (!this.stopped) {
      // `concurrency` counts calls, so the calls in flight *are* the running
      // promises — a batch holding 256 tasks is one of them.
      const free = concurrency - running.size;
      if (free <= 0) {
        // Wait for a slot rather than spinning. runCall() never rejects, so
        // racing these is safe.
        await Promise.race([...running]);
        continue;
      }
      if (this.planner.empty) {
        await this.idle(pollMs);
        continue;
      }
      let drawn: Draw[] | undefined;
      try {
        drawn = await this.store.claimSession(
          // Only what this worker can run. Queues do not partition work by task
          // name, so another worker's tasks would otherwise be claimed here and
          // failed for want of a handler.
          { queues: this.queues, workerId: this.ownId, leaseMs, names: this.planner.names },
          (claim) => this.planner.plan(free, claim, (rows) => this.deliveries(rows)),
        );
      } catch (err) {
        // A claim can fail transiently (lock contention, a dropped connection).
        // Report it and keep polling — one bad poll must not end the worker.
        this.report(err, { phase: "claim" });
        await this.sleepOrStop(CLAIM_ERROR_BACKOFF_MS);
        continue;
      }
      if (!drawn?.length) {
        await this.idle(pollMs);
        continue;
      }
      for (const { src, calls } of drawn) {
        for (const [reg, group] of calls) {
          // Charged before the handler starts and refunded when it settles, so
          // the resource covers exactly the span the call holds its slot.
          const resource = src.resource;
          this.planner.acquire(resource);
          // An unregistered name has nothing to run, so it never starts a handler
          // or a heartbeat; everything else is one call, batched or not.
          const call =
            reg == null ? this.failNoHandler(group[0], leaseMs) : this.runCall(reg, group, leaseMs);
          const p = call.finally(() => {
            this.planner.release(resource);
            running.delete(p);
          });
          running.add(p);
        }
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

  /**
   * Run the worker in the same process for the duration of fn (deployment mode A).
   *
   * `run()` can fail before `fn` has even started — a database that will not
   * open, a migration that will not apply, a protocol version this SDK does not
   * speak — so its rejection is captured from the first tick rather than only at
   * the `await` in the teardown. Left uncaught until then it is an unhandled
   * rejection, which Node terminates the process for by default: the caller
   * never saw the error, and `fn` never finished. (The same mistake `runCall`
   * documents having made.)
   *
   * If `fn` returned, the worker's failure is what the caller hears: a value
   * produced with no worker running is not a success worth reporting quietly. If
   * `fn` threw, its own error wins — it is the caller's code, and replacing it
   * would hide what they were actually doing — with the worker's failure
   * attached as `cause` so the root of it stays reachable.
   */
  async background<T>(fn: () => Promise<T>, opts: { concurrency?: number } = {}): Promise<T> {
    const runner = this.run(opts);
    // Attached now, not at the await in the teardown: that is what keeps an
    // early rejection from being an unhandled one. Carried as a value rather
    // than assigned to a captured variable, so the outcome flows out of
    // `shutdown` and needs no cast to be read.
    const settled = runner.then(
      () => null,
      (err: unknown) => ({ err }),
    );
    const shutdown = async (): Promise<{ err: unknown } | null> => {
      this.stop();
      const failure = await settled;
      await this.closeIfOwned();
      return failure;
    };
    let value: T;
    try {
      value = await fn();
    } catch (err) {
      const failure = await shutdown();
      if (failure && err instanceof Error && err.cause === undefined) err.cause = failure.err;
      throw err;
    }
    const failure = await shutdown();
    if (failure) throw failure.err;
    return value;
  }

  /**
   * Record a claimed task this worker cannot run. Reachable only if a name is
   * unregistered mid-run — the claim filters on the registered names — so it does
   * not start a handler or a heartbeat for a task it will not run.
   */
  private async failNoHandler(task: Task, leaseMs: number): Promise<void> {
    try {
      await this.safeFail(
        this.context(task, leaseMs),
        errorEnvelope({
          type: "NoHandler",
          code: "no_handler",
          message: `no handler registered for ${task.name}`,
          retryable: false,
        }),
        false,
      );
    } catch (err) {
      this.report(err, { phase: "execute", taskId: task.id });
    }
  }

  /**
   * Run one handler call to completion — one task, or a whole batch. Never
   * rejects: a task-level failure is reported through onError and the loop moves
   * on. (It used to reject into a promise nobody awaited — an unhandled rejection
   * that took the process down.)
   *
   * One lifecycle for both delivery modes, because a single-task handler *is* the
   * one-element case: the same heartbeat covers the call, the same classifier
   * reads its error, and the same rule settles what is left. Only two things vary
   * — how the handler is invoked, and where a leftover task's result comes from —
   * so those are the only two branches below.
   *
   * The contract is single: **when the handler returns, every task it did not
   * settle itself is settled by how the call ended.** Returning succeeds them,
   * throwing fails them (retryably, or as the thrown TaskError says). That is
   * what keeps the ordinary cases free of bookkeeping — a handler that just
   * returns has finished 256 tasks — while still letting it pick individual
   * tasks off with `item.succeed()` / `item.fail()` as it goes.
   *
   * For a batch, a returned map of task id -> result fills in results for the
   * tasks left over; anything else returned is ignored, and unmentioned tasks
   * succeed with no result (the common shape, where the handler's output went to
   * a database rather than into the task row). For a single task the return value
   * simply is the result.
   */
  private async runCall(reg: Registration, tasks: Task[], leaseMs: number): Promise<void> {
    const ctxs = tasks.map((t) => this.context(t, leaseMs));
    const batched = reg.batch != null;
    const hb = this.startHeartbeat(ctxs, leaseMs);
    try {
      let result: unknown;
      try {
        result = await this.attempt(
          () =>
            batched
              ? (reg.fn as BatchHandler)(ctxs)
              : (reg.fn as Handler)(ctxs[0], tasks[0].payload),
          ctxs,
        );
      } catch (err) {
        const outcome = this.outcomeOf(err, tasks[0].name);
        if (outcome) await this.settleEach(ctxs, (c) => this.safeFail(c, outcome[0], outcome[1]));
        return;
      }
      // A batch handler's return maps task id -> result; a single-task handler's
      // return *is* the result. Anything else a batch returns is ignored, so its
      // leftovers succeed with no result — hence the empty map rather than
      // falling through to `result`.
      const results = batched
        ? result != null && typeof result === "object"
          ? (result as Record<string, unknown>)
          : {}
        : null;
      await this.settleEach(ctxs, (c) =>
        this.succeedOne(c, results ? (results[c.taskId] ?? null) : result),
      );
    } catch (err) {
      this.report(err, { phase: "execute", taskId: tasks[0].id });
    } finally {
      hb.cancel();
      await hb.done;
    }
  }

  /**
   * How an attempt that ended badly is recorded: [envelope, retryable], or null
   * when there is nothing to record.
   *
   * One classifier for both delivery modes, so a handler error cannot mean
   * different things depending on how its task happened to be delivered. That
   * includes LostLease, which is not an outcome at all: it means a write through
   * this context was already rejected, so recording anything more would be
   * rejected too. Both modes then leave the task alone — the single-task one has
   * nothing else to do, and a batch lets its remaining tasks fall to lease expiry
   * and redelivery rather than stamping them with a failure the handler never
   * reported.
   */
  private outcomeOf(err: unknown, name: string): [Record<string, unknown>, boolean] | null {
    if (err instanceof LostLease) return null;
    // Retryable, so backoff / maxAttempts / cancel-wins all apply exactly as for
    // a thrown error.
    if (err instanceof AttemptTimeout) return [timeoutEnvelope(name, err.maxRunMs), true];
    // Everything else is what a handler could equally have passed to ctx.fail(),
    // so it goes through the same normalizer — a TaskError keeps its own
    // retryability, anything else is retryable. Non-Error throws reach
    // exceptionEnvelope the same way, since only ctx.fail can supply a ready
    // envelope and a thrown object is not one.
    if (err !== null && typeof err === "object" && !(err instanceof Error)) {
      return [exceptionEnvelope(err), true];
    }
    return asEnvelope(err as FailReason, true);
  }

  // Both leftover paths write through the store rather than through the context.
  // `TaskContext.owned` short-circuits once a lease is known lost, which is there
  // to stop a *zombie handler* writing after its attempt was abandoned — but
  // these run after the handler is done, on the worker's own authority, exactly
  // as execute()'s completion and failure arms do. Ownership is still enforced by
  // each statement, so a task whose lease really was lost writes nothing either
  // way.

  /**
   * Finalize one task the handler left for the worker to decide — the tail of
   * both delivery modes.
   *
   * Includes the unserializable-result rule: the handler succeeded but its value
   * cannot cross the JSON protocol (BigInt, non-finite number, circular), which
   * is deterministic, so it fails permanently rather than being redelivered to
   * fail the same way every attempt.
   */
  private async succeedOne(ctx: TaskContext, result: unknown): Promise<void> {
    if (ctx.settled) return;
    try {
      // complete (not succeed): finalizes as canceled if a cancel was requested
      // while the handler ran, else succeeded.
      await this.store.complete({ taskId: ctx.taskId, workerId: this.ownId, result });
      ctx.markSettled();
    } catch (err) {
      if (err instanceof LostLease) {
        ctx.markLeaseLost();
        return;
      }
      if (err instanceof SerializationError) {
        await this.safeFail(
          ctx,
          errorEnvelope({
            type: "SerializationError",
            code: "unserializable_result",
            message: `handler result is not JSON-serializable: ${err.message}`,
            retryable: false,
          }),
          false,
        );
        return;
      }
      throw err;
    }
  }

  /**
   * Settle every task the handler did not settle itself, concurrently, reporting
   * rather than throwing. Each task keeps its own attempt count and backoff —
   * they are separate tasks that happened to be delivered together.
   *
   * allSettled, because one task's write failing must not abandon the rest of the
   * batch mid-settlement — the others still hold leases and would sit `running`
   * until expiry. Each outcome is reported against the task it belongs to;
   * without that, an operator learns a settlement failed somewhere in a batch of
   * 256.
   */
  private async settleEach(
    ctxs: TaskContext[],
    settle: (ctx: TaskContext) => Promise<void>,
  ): Promise<void> {
    const left = ctxs.filter((c) => !c.settled);
    if (!left.length) return;
    const outcomes = await Promise.allSettled(left.map(settle));
    outcomes.forEach((outcome, i) => {
      if (outcome.status === "rejected") {
        this.report(outcome.reason, { phase: "execute", taskId: left[i].taskId });
      }
    });
  }

  /**
   * Run one attempt — of one task or of a whole batch — bounded by maxRunMs when
   * set.
   *
   * On timeout the attempt is abandoned: every context it covers is flagged
   * lease-lost first (ctx.signal aborts; a handler that keeps running can never
   * write to its task again, nor settle anything behind the worker's back — see
   * TaskContext.owned — and cannot create child tasks either, which would
   * otherwise be created a second time by the retry), then the still-pending
   * promise is left to settle on its own, its outcome discarded. What a zombie
   * does OUTSIDE cairnq is its own business: `ctx.signal` is how a handler is
   * told to stop, and only the handler can act on it. The caller records the handler_timeout failure;
   * lease recovery is NOT involved, so redelivery is immediate.
   */
  private async attempt(invoke: () => unknown, ctxs: TaskContext[]): Promise<unknown> {
    const maxRunMs = this.opts.maxRunMs;
    if (maxRunMs == null) return invoke();
    // As a real promise: the race needs one (a handler may return a plain
    // value), and the timeout path .catch()es it.
    const run = (async () => invoke())();
    let timer!: NodeJS.Timeout;
    const winner = await Promise.race([
      run,
      new Promise<typeof TIMED_OUT>((r) => (timer = setTimeout(() => r(TIMED_OUT), maxRunMs))),
    ]).finally(() => clearTimeout(timer));
    if (winner !== TIMED_OUT) return winner;
    for (const ctx of ctxs) ctx.markLeaseLost();
    // The zombie may still reject later; that must not become an unhandled
    // rejection — its outcome was already decided to be handler_timeout.
    run.catch(() => {});
    throw new AttemptTimeout(maxRunMs);
  }

  /**
   * One statement per beat, however many tasks the call covers — a single-task
   * handler is just the one-element case.
   *
   * Only tasks still in play are renewed. A task the handler already settled is
   * terminal, and re-leasing it would be a write against a row nobody owns; that
   * is also why an absence is only read as lease loss after re-checking
   * `settled`, since the handler may have finalized the task while this beat was
   * in flight, which takes the row out of `running` and out of the reply. A task
   * genuinely missing lost its lease (another worker recovered it), so its
   * context is flagged and the handler stops being able to write through it —
   * only that one, never its neighbours.
   */
  private startHeartbeat(
    ctxs: TaskContext[],
    leaseMs: number,
  ): { cancel: () => void; done: Promise<void> } {
    let active = true;
    let wake: (() => void) | null = null;
    // lease/3 gives two beats of slack; the floor only matters for sub-150ms leases.
    const interval = Math.max(50, Math.floor(leaseMs / 3));

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
        const live = ctxs.filter((c) => !c.settled && !c.lostLease);
        if (!live.length) break;
        try {
          const renewed = await this.store.heartbeatBatch({
            taskIds: live.map((c) => c.taskId),
            workerId: this.ownId,
            leaseMs,
          });
          for (const ctx of live) {
            const cancelRequested = renewed.get(ctx.taskId);
            // Cancellation rides along on the write we were making anyway, so
            // ctx.canceled() stays free here too.
            if (cancelRequested !== undefined) ctx.observeCancel(cancelRequested);
            else if (!ctx.settled) ctx.markLeaseLost();
          }
        } catch (err) {
          this.report(err, { phase: "execute", taskId: live[0].taskId });
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
    ctx: TaskContext,
    envelope: Record<string, unknown>,
    retryable: boolean,
  ): Promise<void> {
    try {
      await this.store.fail({
        taskId: ctx.taskId,
        workerId: this.ownId,
        error: envelope,
        retryable,
        delayMs: failDelayMs(ctx.attempt, retryable, this.backoffMs, this.backoffMaxMs),
      });
      ctx.markSettled();
    } catch (err) {
      if (!(err instanceof LostLease)) throw err;
    }
  }

  /**
   * The empty-poll sleep. A store with a push channel (Postgres LISTEN/NOTIFY)
   * cuts it short when a task on this worker's queues becomes claimable;
   * stop() interrupts it either way, and sleepOrStop bounds it at `ms` so the
   * poll fallback — which also drives lease recovery — never stretches.
   */
  private idle(ms: number): Promise<void> {
    return Promise.race([this.sleepOrStop(ms), this.store.claimWake(this.queues, ms)]);
  }

  private sleepOrStop(ms: number): Promise<void> {
    if (this.stopped) return Promise.resolve();
    let timer!: NodeJS.Timeout;
    const nap = new Promise<void>((r) => (timer = setTimeout(r, ms)));
    // Clear the timer whichever side wins, so a stop is never followed by a
    // leftover poll timer holding the process open.
    return Promise.race([nap, this.stopped$]).finally(() => clearTimeout(timer));
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
