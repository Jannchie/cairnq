import { newId } from "../ids.js";
import {
  AlreadyExists,
  errorEnvelope,
  LostLease,
  ProtocolVersionMismatch,
  SerializationError,
} from "../errors.js";
import { rowToTask, STATUSES, type Task, type TaskStatus } from "../models.js";

const rejectMangled = function (this: unknown, _key: string, v: unknown): unknown {
  if (typeof v === "number" && !Number.isFinite(v)) {
    throw new SerializationError(`non-finite number ${v} is not JSON-serializable`);
  }
  // In an array these become the literal `null` (in an object they are merely
  // omitted, the JS idiom for "absent") — the twin SDK would read back a null
  // the caller never wrote.
  if (Array.isArray(this) && (v === undefined || typeof v === "function" || typeof v === "symbol")) {
    throw new SerializationError(`${typeof v} inside an array is not JSON-serializable`);
  }
  return v;
};

/** Encode a value for a protocol JSON column, raising SerializationError on
 * anything JSON cannot represent. Refuses what JSON.stringify would silently
 * mangle into `null`: NaN/Infinity anywhere, undefined/function/symbol inside an
 * array, and a top-level undefined that disappears entirely — either way the
 * twin SDK reads back something other than what the caller meant (the Python
 * SDK rejects the same values, via allow_nan=False). */
export function dumpJson(value: unknown): string {
  let text: string | undefined;
  try {
    text = JSON.stringify(value);
  } catch (err) {
    // BigInt or a circular structure.
    throw new SerializationError(err instanceof Error ? err.message : String(err));
  }
  if (text === undefined) {
    throw new SerializationError(`value of type ${typeof value} is not JSON-serializable`);
  }
  // Every mangled value reaches the output as the literal `null`, so a
  // null-free result needs no strict pass — this keeps the replacer (which
  // forfeits V8's native stringifier) off the hot path.
  if (text.includes("null")) JSON.stringify(value, rejectMangled);
  return text;
}

const SUPPORTED_PROTOCOL_MAJOR = 1;

/** Refuse to run against a store whose protocol major this SDK does not speak.
 * The supported major is a protocol fact, not a dialect one — every backend
 * checks it here so the constant can't fork per store. */
export function checkProtocolVersion(version: number): void {
  if (version !== SUPPORTED_PROTOCOL_MAJOR) {
    throw new ProtocolVersionMismatch(
      `storage protocol_version=${version}, SDK supports ${SUPPORTED_PROTOCOL_MAJOR}`,
    );
  }
}

// CONFLICTS is the canonical declaration; the type derives from it so the
// runtime guard in submit() and the type can't drift apart (same pattern as
// STATUSES/TaskStatus in models.ts).
const CONFLICTS = ["reuse", "reject", "replace"] as const;
export type Conflict = (typeof CONFLICTS)[number];

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
  status?: TaskStatus | null;
  queue?: string | null;
  name?: string | null;
  rootId?: string | null;
  correlationId?: string | null;
  limit?: number;
  offset?: number;
}

export interface PurgeInput {
  olderThanMs?: number;
  limit?: number;
}

export type Params = Record<string, unknown>;
/** Runs one named protocol statement and returns its rows. */
export type Fetch = (name: string, params: Params) => Promise<any[]>;

export const LEASE_EXPIRED_ERROR_JSON = dumpJson(
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
  /**
   * Run several statements atomically; `fn` receives a Fetch bound to the txn.
   *
   * A backend may invoke `fn` more than once, retrying the transaction after a
   * transient failure (SQLite does, on write-lock contention). So `fn` must be
   * replayable: derive nothing inside it that the caller cannot derive twice —
   * build ids and payloads before opening the transaction, not within it.
   */
  protected abstract tx<T>(fn: (fetch: Fetch) => Promise<T>): Promise<T>;

  /**
   * Whether it is worth opening the claim transaction at all. SQLite gates its
   * single write lock behind a read-only probe; Postgres readers don't block
   * writers, so it just says yes.
   */
  protected async hasClaimableWork(_params: Params): Promise<boolean> {
    return true;
  }

  // ------------------------------------------------------------ wake channel
  // Wake-or-timeout contract (PROTOCOL.md "Push wakeups"): resolve when the
  // watched event may have happened, or after timeoutMs at the latest. The
  // default is a plain sleep — polling IS the wake mechanism; a dialect with a
  // push channel (PostgresStore, LISTEN/NOTIFY) resolves earlier.

  /** Resolves when a task may have become claimable on one of `queues`. The
   * timer is unref'd: the worker races this against its own stop-aware, ref'd
   * sleep, so it must neither hold the process open nor need clearing. */
  claimWake(_queues: string[], timeoutMs: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, timeoutMs).unref?.());
  }

  /** Resolves when `taskId` may have gone terminal. Plain ref'd sleep —
   * pollWait awaits it directly, so it is what keeps the process alive. */
  taskDoneWake(_taskId: string, timeoutMs: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, timeoutMs));
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
      payload: dumpJson(input.payload ?? {}),
      metadata: dumpJson(input.metadata ?? {}),
      max_attempts: input.maxAttempts ?? 3,
      priority: input.priority ?? 0,
      delay_ms: input.runAtDelayMs ?? 0,
      parent_id: input.parentId ?? null,
      root_id: input.rootId ?? id,
      correlation_id: input.correlationId ?? null,
    };
    const key = input.key ?? null;
    const conflict = input.conflict ?? "reuse";
    // Validate up front: untyped callers otherwise only hit the strategy branch
    // on the second submit of a key, deep inside the transaction.
    if (!CONFLICTS.includes(conflict)) {
      throw new Error(`unknown conflict strategy: ${conflict}`);
    }
    // maxAttempts < 1 would still run once (claim increments before the check),
    // a silently different meaning than the number says; a negative delay is
    // always a mistake. Both fail loudly instead. Only supplied values are
    // checked — the defaults live in the params object alone.
    if (input.maxAttempts != null && input.maxAttempts < 1) {
      throw new Error(`maxAttempts must be >= 1, got ${input.maxAttempts}`);
    }
    if (input.runAtDelayMs != null && input.runAtDelayMs < 0) {
      throw new Error(`runAtDelayMs must be >= 0, got ${input.runAtDelayMs}`);
    }
    if (key === null) return rowToTask((await this.fetch("insert_task", ins))[0]);

    // A key makes submit a read-then-write, so it has to be one transaction —
    // opened by taking the key's lock, because on Postgres the transaction alone
    // is not enough: concurrent same-key submits must not both see "no existing
    // task" (see lock_key.sql; on SQLite it is a no-op).
    return this.tx(async (fetch) => {
      await fetch("lock_key", { key });
      const existing = (await fetch("get_key", { key })) as { task_id: string }[];
      if (existing.length) {
        // Read the task itself before branching: a concurrent purge (which
        // takes no key lock) may have deleted it — cascading the key row away —
        // between our statements' snapshots. A vanished task means the key is
        // free after all, whatever the strategy.
        const current = (await fetch("get", { id: existing[0].task_id }))[0];
        if (current) {
          if (conflict === "reuse") return rowToTask(current);
          if (conflict === "reject") throw new AlreadyExists(key);
          // "replace": cancel the recorded task, then repoint the key below.
          await fetch("cancel", { id: existing[0].task_id });
        }
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
    // Validate up front, like submit's conflict guard: a typo'd status otherwise
    // matches nothing and returns [] indistinguishably from "no such tasks".
    if (input.status != null && !STATUSES.includes(input.status)) {
      throw new Error(`unknown status filter: ${input.status}`);
    }
    if ((input.limit != null && input.limit < 0) || (input.offset != null && input.offset < 0)) {
      throw new Error(`limit/offset must be >= 0, got limit=${input.limit} offset=${input.offset}`);
    }
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
   * Resolve a key to the task it currently points at, then act on that task —
   * under the key's lock, so a concurrent `replace` can't repoint the key
   * between the lookup and the write (the transaction alone is not enough on
   * Postgres; see lock_key.sql).
   */
  private async byKey(name: string, key: string, params: Params): Promise<Task | null> {
    return this.tx(async (fetch) => {
      await fetch("lock_key", { key });
      const existing = (await fetch("get_key", { key })) as { task_id: string }[];
      if (!existing.length) return null;
      return TaskStore.one(await fetch(name, { id: existing[0].task_id, ...params }));
    });
  }

  /**
   * Delete terminal tasks that completed more than `olderThanMs` ago and return
   * their ids. Nothing else removes rows, so a long-lived database needs this
   * called periodically. Bounded by `limit` to keep each sweep a short write;
   * call it in a loop until it returns fewer than `limit`.
   */
  async purge(input: PurgeInput = {}): Promise<string[]> {
    if (input.olderThanMs != null && input.olderThanMs < 0) {
      throw new Error(`olderThanMs must be >= 0, got ${input.olderThanMs}`);
    }
    if (input.limit != null && input.limit < 1) {
      throw new Error(`limit must be >= 1, got ${input.limit}`);
    }
    const rows = await this.fetch("purge", {
      older_than_ms: input.olderThanMs ?? 0,
      limit: input.limit ?? 1_000,
    });
    return rows.map((r) => r.id as string);
  }

  /**
   * Task counts per queue, keyed by status and zero-filled across all statuses —
   * `(await stats()).default.queued` is the backlog of a queue. A queue appears
   * only while it has rows; terminal tasks keep counting until `purge` removes
   * them.
   */
  async stats(): Promise<Record<string, Record<TaskStatus, number>>> {
    const out: Record<string, Record<TaskStatus, number>> = {};
    for (const row of await this.fetch("stats", {})) {
      const per = (out[row.queue] ??= Object.fromEntries(
        STATUSES.map((s) => [s, 0]),
      ) as Record<TaskStatus, number>);
      per[row.status as TaskStatus] = Number(row.count);
    }
    return out;
  }

  /**
   * How many more tasks fit on `queue` under `maxDepth` — 0 once it is full.
   *
   * The cheap half of backpressure: bounded at `maxDepth` index entries, unlike
   * `stats()`, which aggregates the whole table (terminal rows included) and so
   * costs more the longer a database has been running. Use it directly to shed
   * load or shape a producer; `QueueDepthGate` builds the blocking form on top.
   */
  async queueDepth(queue: string, maxDepth: number): Promise<number> {
    if (!Number.isInteger(maxDepth) || maxDepth < 0) {
      throw new Error(`maxDepth must be a non-negative integer, got ${maxDepth}`);
    }
    const rows = await this.fetch("queue_depth", { queue, max_depth: maxDepth });
    return Number(rows[0]?.headroom ?? 0);
  }

  // ------------------------------------------------------------- worker side
  /**
   * Take up to `limit` claimable tasks. `names` restricts the claim to task names
   * this caller can actually run — a worker passes its registered handlers.
   * Queues alone do not partition work, so without it a worker claims a task it
   * cannot run and fails it permanently. Undefined means no filter; an empty
   * array claims nothing.
   */
  async claim(input: {
    queues: string[];
    workerId: string;
    leaseMs?: number;
    limit?: number;
    names?: string[];
  }): Promise<Task[]> {
    // One queue is the common case and gets its own statement: a list-valued queue
    // filter cannot be read in claim order, so the planner sorts every claimable
    // row to take LIMIT of them, and claim's cost grows with the queued backlog
    // while it holds the claim transaction. See claim_one_queue.sql.
    const oneQueue = input.queues.length === 1;
    const params: Params = {
      queues: input.queues,
      queue: oneQueue ? input.queues[0] : null,
      names: input.names ?? null,
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
      return (await fetch(oneQueue ? "claim_one_queue" : "claim", params)).map(rowToTask);
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
      result: input.result == null ? null : dumpJson(input.result),
      message: null,
    });
  }

  async complete(input: { taskId: string; workerId: string; result: unknown }): Promise<Task> {
    return this.ownedWrite("complete", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      result: input.result == null ? null : dumpJson(input.result),
    });
  }

  async fail(input: {
    taskId: string;
    workerId: string;
    error: unknown;
    retryable?: boolean;
    delayMs?: number;
  }): Promise<Task> {
    let error: string;
    try {
      error = dumpJson(input.error ?? {});
    } catch (err) {
      if (!(err instanceof SerializationError)) throw err;
      // A failure record must never itself fail to serialize (a TaskError
      // carrying exotic details would otherwise strand the task until lease
      // expiry). Strip the envelope to its string fields and record that.
      const e = (input.error ?? {}) as { type?: unknown; code?: unknown; message?: unknown };
      error = dumpJson(
        errorEnvelope({
          type: String(e.type ?? "TaskError"),
          code: String(e.code ?? "task_error"),
          message: String(e.message ?? ""),
          retryable: input.retryable !== false,
        }),
      );
    }
    return this.ownedWrite("fail", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      error,
      retryable: input.retryable !== false,
      delay_ms: input.delayMs ?? 0,
    });
  }
}
