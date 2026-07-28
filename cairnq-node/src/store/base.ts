import { newId } from "../ids.js";
import { AlreadyExists, errorEnvelope, LostLease } from "../errors.js";
import { rowToTask, type Task } from "../models.js";

export type Conflict = "reuse" | "reject" | "replace";

export interface SubmitInput {
  name: string;
  payload: unknown;
  queue?: string;
  key?: string | null;
  conflict?: Conflict;
  maxAttempts?: number;
  priority?: number;
  metadata?: unknown;
  parentId?: string | null;
  rootId?: string | null;
  correlationId?: string | null;
  runAtDelayMs?: number;
}

export interface ListInput {
  status?: string | null;
  queue?: string | null;
  name?: string | null;
  rootId?: string | null;
  correlationId?: string | null;
  limit?: number;
  offset?: number;
}

export type Params = Record<string, unknown>;
/** Runs one named protocol statement and returns its rows. */
export type Fetch = (name: string, params: Params) => Promise<any[]>;

export const LEASE_EXPIRED_ERROR_JSON = JSON.stringify(
  errorEnvelope({
    type: "LeaseExpired",
    code: "lease_expired",
    message: "task lease expired and max attempts reached",
    retryable: false,
  }),
);

/** Strips SQL line comments, so a `:name` in a header comment isn't a parameter. */
export const COMMENT = /--[^\n]*/g;
/** A `:name` placeholder. The lookbehind spares Postgres `::type` casts. */
export const NAMED = /(?<!:):(\w+)/g;

// Statement text is loaded once at construction and never varies, so the parse is
// memoized on it: every dialect's binding path runs on each query, and re-scanning
// the SQL each time would put a regex sweep on the worker's poll loop.
const paramCache = new Map<string, readonly string[]>();

/**
 * The parameter names a statement binds, in first-appearance order.
 *
 * Callers pass a superset of parameters and each dialect takes what its own SQL
 * asks for — that is what lets one call site serve both dialects even though e.g.
 * SQLite binds `:lease_until_ms` where Postgres binds `:lease_ms`. This is the one
 * place that decides what counts as a parameter; both dialects' binding goes
 * through it.
 */
export function statementParams(sql: string): readonly string[] {
  let names = paramCache.get(sql);
  if (!names) {
    const seen = new Set<string>();
    for (const m of sql.replace(COMMENT, "").matchAll(NAMED)) seen.add(m[1]);
    names = [...seen];
    paramCache.set(sql, names);
  }
  return names;
}

/**
 * The storage seam.
 *
 * A backend supplies three things: how to run one protocol statement, how to run
 * several inside a transaction, and how its dialect binds parameters. Everything
 * above that — the submit conflict branches, the *_by_key lookups, the
 * recover-then-claim sequence, the ownership-checked writes — lives here once,
 * because those are protocol decisions rather than storage decisions. Keeping
 * them in one place is what stops SQLite and Postgres from drifting apart in
 * behavior; the shared SQL already stops them from drifting in wording.
 */
export abstract class TaskStore {
  // ------------------------------------------------------------ dialect seam
  abstract connect(): Promise<void>;
  abstract close(): Promise<void>;
  abstract protocolVersion(): Promise<number>;

  /** Run one protocol statement outside a transaction, connecting if needed. */
  protected abstract fetch(name: string, params: Params): Promise<any[]>;
  /** Run several statements atomically; `fn` receives a Fetch bound to the txn. */
  protected abstract tx<T>(fn: (fetch: Fetch) => Promise<T>): Promise<T>;

  /**
   * Whether it is worth opening the claim transaction at all. SQLite gates its
   * single write lock behind a read-only probe; Postgres readers don't block
   * writers, so it just says yes.
   */
  protected async hasClaimableWork(_params: Params): Promise<boolean> {
    return true;
  }

  // --------------------------------------------------------------- internals
  /**
   * An ownership-checked worker write (heartbeat/progress/succeed/complete/fail).
   * Each statement's WHERE pins worker_id + a live lease, so 0 rows back means
   * the lease was lost — every such write reports it the same way.
   */
  private async ownedWrite(name: string, taskId: string, params: Params): Promise<Task> {
    const rows = await this.fetch(name, params);
    if (!rows.length) throw new LostLease(taskId);
    return rowToTask(rows[0]);
  }

  private static one(rows: any[]): Task | null {
    return rows.length ? rowToTask(rows[0]) : null;
  }

  // ------------------------------------------------------------- client side
  async submit(input: SubmitInput): Promise<Task> {
    const id = newId("task");
    const ins: Params = {
      id,
      name: input.name,
      queue: input.queue ?? "default",
      payload: JSON.stringify(input.payload ?? {}),
      metadata: JSON.stringify(input.metadata ?? {}),
      max_attempts: input.maxAttempts ?? 3,
      priority: input.priority ?? 0,
      delay_ms: input.runAtDelayMs ?? 0,
      parent_id: input.parentId ?? null,
      root_id: input.rootId ?? id,
      correlation_id: input.correlationId ?? null,
    };
    const key = input.key ?? null;
    const conflict = input.conflict ?? "reuse";
    if (key === null) return rowToTask((await this.fetch("insert_task", ins))[0]);

    // A key makes submit a read-then-write, so it has to be one transaction:
    // concurrent same-key submits must not both see "no existing task".
    return this.tx(async (fetch) => {
      const existing = (await fetch("get_key", { key })) as { task_id: string }[];
      if (existing.length) {
        const exId = existing[0].task_id;
        if (conflict === "reuse") return rowToTask((await fetch("get", { id: exId }))[0]);
        if (conflict === "reject") throw new AlreadyExists(key);
        if (conflict !== "replace") throw new Error(`unknown conflict strategy: ${conflict}`);
        await fetch("cancel", { id: exId });
      }
      const row = (await fetch("insert_task", ins))[0];
      await fetch("upsert_key", { key, task_id: id });
      return rowToTask(row);
    });
  }

  async get(taskId: string): Promise<Task | null> {
    return TaskStore.one(await this.fetch("get", { id: taskId }));
  }

  async getByKey(key: string): Promise<Task | null> {
    return TaskStore.one(await this.fetch("get_by_key", { key }));
  }

  async list(input: ListInput = {}): Promise<Task[]> {
    const rows = await this.fetch("list", {
      status: input.status ?? null,
      queue: input.queue ?? null,
      name: input.name ?? null,
      root_id: input.rootId ?? null,
      correlation_id: input.correlationId ?? null,
      limit: input.limit ?? 100,
      offset: input.offset ?? 0,
    });
    return rows.map(rowToTask);
  }

  async cancel(taskId: string): Promise<Task | null> {
    return TaskStore.one(await this.fetch("cancel", { id: taskId }));
  }

  async retry(taskId: string, opts: { resetAttempt?: boolean } = {}): Promise<Task | null> {
    return TaskStore.one(
      await this.fetch("retry", { id: taskId, reset_attempt: opts.resetAttempt ?? false }),
    );
  }

  async cancelByKey(key: string): Promise<Task | null> {
    return this.byKey("cancel", key, {});
  }

  async retryByKey(key: string, opts: { resetAttempt?: boolean } = {}): Promise<Task | null> {
    return this.byKey("retry", key, { reset_attempt: opts.resetAttempt ?? false });
  }

  /**
   * Resolve a key to the task it currently points at, then act on that task — in
   * one transaction, so a concurrent `replace` can't repoint the key between the
   * lookup and the write.
   */
  private async byKey(name: string, key: string, params: Params): Promise<Task | null> {
    return this.tx(async (fetch) => {
      const existing = (await fetch("get_key", { key })) as { task_id: string }[];
      if (!existing.length) return null;
      return TaskStore.one(await fetch(name, { id: existing[0].task_id, ...params }));
    });
  }

  // ------------------------------------------------------------- worker side
  async claim(input: {
    queues: string[];
    workerId: string;
    leaseMs?: number;
    limit?: number;
  }): Promise<Task[]> {
    const params: Params = {
      queues: input.queues,
      worker_id: input.workerId,
      lease_ms: input.leaseMs ?? 30_000,
      limit: input.limit ?? 1,
      lease_expired_error: LEASE_EXPIRED_ERROR_JSON,
    };
    if (!(await this.hasClaimableWork(params))) return [];
    // Recovery must share the claim's transaction: a lease reclaimed here has to
    // be visible to the claim that follows, and to nobody in between.
    return this.tx(async (fetch) => {
      await fetch("recover_leases", params);
      return (await fetch("claim", params)).map(rowToTask);
    });
  }

  async heartbeat(input: { taskId: string; workerId: string; leaseMs?: number }): Promise<Task> {
    return this.ownedWrite("heartbeat", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      lease_ms: input.leaseMs ?? 30_000,
    });
  }

  async progress(input: {
    taskId: string;
    workerId: string;
    progress: number | null;
    message: string | null;
  }): Promise<Task> {
    return this.ownedWrite("progress", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      progress: input.progress,
      message: input.message,
    });
  }

  async succeed(input: { taskId: string; workerId: string; result: unknown }): Promise<Task> {
    return this.ownedWrite("succeed", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      result: input.result == null ? null : JSON.stringify(input.result),
      message: null,
    });
  }

  async complete(input: { taskId: string; workerId: string; result: unknown }): Promise<Task> {
    return this.ownedWrite("complete", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      result: input.result == null ? null : JSON.stringify(input.result),
    });
  }

  async fail(input: {
    taskId: string;
    workerId: string;
    error: unknown;
    retryable?: boolean;
    delayMs?: number;
  }): Promise<Task> {
    return this.ownedWrite("fail", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      error: JSON.stringify(input.error ?? {}),
      retryable: input.retryable !== false,
      delay_ms: input.delayMs ?? 0,
    });
  }
}
