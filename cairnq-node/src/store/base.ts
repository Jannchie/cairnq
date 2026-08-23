import { newId } from "../ids.js";
import {
  AlreadyExists,
  errorEnvelope,
  LostLease,
  ProtocolVersionMismatch,
  SerializationError,
  UnsupportedBackend,
} from "../errors.js";
import {
  isTerminalStatus,
  rowToRef,
  rowToTask,
  STATUSES,
  type Task,
  type TaskRef,
  type TaskStatus,
} from "../models.js";
import { type BackpressureOptions, QueueDepthGate } from "../backpressure.js";

/**
 * Whether `JSON.stringify` would write this object without its contents.
 *
 * Every such value serializes to `{}` — or, for a typed array, to an object
 * keyed by index — with everything it actually held silently gone. A handler
 * that returns a `Map` would otherwise record `{}` as the task's result:
 * succeeded, no error, and nothing left to recover the value from. That is the
 * same class of silent mangle as `NaN` becoming `null`, so it is refused the
 * same way.
 *
 * Structural rather than a list of built-in names, because a list is only ever
 * as current as the day it was written. An enumeration of `Map`/`Set`/`Promise`/
 * the typed arrays already missed `Float16Array` (ES2025, and `engines` here is
 * node >=22), `Blob`, `Headers`, `URLSearchParams`, and — unfixably, since its
 * tag is plain `[object Object]` — a class whose state is all private fields.
 * The rule below catches those, and whatever the platform adds next, with
 * nothing to maintain:
 *
 * - **A non-plain object with no own enumerable properties.** Its state lives
 *   somewhere `JSON.stringify` cannot reach (internal slots, private fields, a
 *   host binding), so `{}` is all that would be written. A *plain* `{}` is
 *   deliberately exempt: an empty object is a value, not a loss.
 * - **Any `ArrayBuffer` view.** A typed array writes its indices as string keys
 *   and reads back as an object, not an array — non-empty, so the first rule
 *   does not see it.
 *
 * A class instance that carries ordinary properties still crosses: those
 * properties are exactly what gets written, and nothing is lost. `Date` never
 * reaches here at all — `toJSON` has already replaced it with its ISO string by
 * the time a replacer is called, which is also why `toJSON` remains the escape
 * hatch for anything that wants to define its own JSON form.
 */
function emptiesItselfOut(v: object): boolean {
  if (ArrayBuffer.isView(v)) return true;
  const proto = Object.getPrototypeOf(v);
  // Plain and null-prototype objects are the two shapes whose own enumerable
  // properties ARE their contents, so an empty one is honestly empty.
  if (proto === Object.prototype || proto === null) return false;
  return Object.keys(v).length === 0;
}

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
  if (v !== null && typeof v === "object" && !Array.isArray(v) && emptiesItselfOut(v)) {
    const name = (v as { constructor?: { name?: string } }).constructor?.name ?? "object";
    throw new SerializationError(
      `${name} carries nothing JSON.stringify can see and would be written as an empty object`,
    );
  }
  return v;
};

/** Encode a value for a protocol JSON column, raising SerializationError on
 * anything JSON cannot represent. Refuses what JSON.stringify would silently
 * mangle: NaN/Infinity anywhere, undefined/function/symbol inside an array, a
 * top-level undefined that disappears entirely, and the objects that would be
 * written as an empty one (see emptiesItselfOut) — either way the twin SDK
 * reads back something other than what the caller meant. The Python SDK rejects
 * the same classes of value; see its dump_json. */
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
  // Every mangled value betrays itself in the output, so the strict pass — which
  // forfeits V8's native stringifier — runs only when one of its traces is
  // there: `null` for a mangled scalar, `{}` for an opaque container emptied
  // out, `{"0":` for a typed array rewritten as an index-keyed object. Each is a
  // superset (a payload carrying an empty object, or a literal "0" key, merely
  // pays for a second pass that then finds nothing), and together they are
  // exhaustive over everything rejectMangled rejects. Measured on a null-free
  // payload the three checks cost ~20% of the encode, against ~130% for running
  // the replacer unconditionally.
  if (text.includes("null") || text.includes("{}") || text.includes('{"0":')) {
    JSON.stringify(value, rejectMangled);
  }
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
const CONFLICTS = ["reuse", "reuse-succeeded", "reject", "replace"] as const;
export type Conflict = (typeof CONFLICTS)[number];

/**
 * Whether a keyed submit's strategy accepts the task the key already points at.
 *
 * Both reuse strategies deduplicate work that is still in play — that is what a
 * key is for, and the answer cannot depend on the outcome of a task that has no
 * outcome yet. They differ only on what a *finished* task means: `reuse` treats
 * the key as free again, while `reuse-succeeded` reads a succeeded task as a
 * cached result. Neither ever hands back a failed or canceled one, which would
 * poison the key for every later submit (see PROTOCOL.md "Key conflict").
 */
function reusable(conflict: Conflict, status: TaskStatus): boolean {
  if (conflict === "replace") return false;
  if (!isTerminalStatus(status)) return true;
  return conflict === "reuse-succeeded" && status === "succeeded";
}

/** The queue a submit lands on when it names none. Owned here, where the
 * default is applied, so nothing above has to re-derive it. */
export const DEFAULT_QUEUE = "default";

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

/** Validate a purge's inputs. Shared with RetentionSweeper, which fail-fasts at
 * construction on the same rules an hourly sweep would otherwise only surface
 * through its onError hook — one statement of the rules, two callers. */
export function validatePurgeInput(input: PurgeInput): void {
  if (input.olderThanMs != null && (!Number.isFinite(input.olderThanMs) || input.olderThanMs < 0)) {
    throw new Error(`olderThanMs must be >= 0, got ${input.olderThanMs}`);
  }
  if (input.limit != null && input.limit < 1) {
    throw new Error(`limit must be >= 1, got ${input.limit}`);
  }
  // Terminal only: purge never deletes live work, so accepting `queued` here
  // would be accepting a filter that silently matches nothing.
  if (input.status != null && !isTerminalStatus(input.status)) {
    throw new Error(`status must be terminal, got ${input.status}`);
  }
}

export interface PurgeInput {
  olderThanMs?: number;
  /** Restrict the sweep to one queue. Absent means every queue.
   *
   * The same tiering argument as `status`, one level up: a single installation
   * is how this project recommends two languages coordinate, so it routinely
   * carries two workloads whose rows have nothing to do with each other's
   * lifetimes — an RPC result read once, a durable job's log kept for a week.
   * Without this the shorter-lived queue sets the retention for both. */
  queue?: string;
  /** Restrict the sweep to one terminal status. Retention needs are tiered —
   * succeeded rows are spent once their result is consumed, failed ones are
   * worth keeping for diagnosis — and without this the shortest-lived tier
   * sets the retention for every row. Absent means all terminal statuses. */
  status?: TaskStatus;
  /** Restrict the sweep to one task name. Absent means all names. */
  name?: string;
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

/**
 * An optional filter: `(:p is null or col = :p)`, with the `::type` cast the
 * Postgres dialect adds to pin the parameter's type. The back-reference is what
 * keeps it from matching anything else — both halves must name the same
 * parameter. claim's `(:names is null or name in (…))` deliberately does not
 * match: a list-valued filter is a different problem, and claim_one_name.sql is
 * its answer.
 */
const OPTIONAL_FILTER = /\(:(\w+)(?:::[\w[\]]+)? is null or (\S+) = :\1\)/g;

// Statement text is loaded once at construction and never varies, so the parse is
// memoized on it: every dialect's binding path runs on each query, and re-scanning
// the SQL each time would put a regex sweep on the worker's poll loop.
const paramCache = new Map<string, readonly string[]>();
// Same argument for the specialized texts, which vary only by WHICH filters a
// caller supplied — a small, bounded set per statement, reached within the first
// few calls and constant thereafter.
const specialCache = new Map<string, string>();

/**
 * The statement as it should run for these arguments: every optional filter the
 * caller actually supplied rewritten to an equality.
 *
 * `(:p is null or col = :p)` cannot use an index. SQLite plans a statement when
 * it is prepared, before any parameter has a value, so it must plan for both
 * branches and settles for a scan; that is not a tuning detail but the whole
 * difference between `list(root_id=…)` seeking cairnq_tasks_root_idx and reading
 * the table. Postgres re-plans with the values for a statement's first
 * executions and folds the branch away on its own, so this is a no-op there —
 * but it costs nothing, and one behaviour is easier to reason about than two.
 *
 * Filters the caller did NOT supply are left alone rather than removed: a
 * constant-true term costs a per-row evaluation the planner mostly discards, and
 * leaving them keeps the parameter set identical to the file's, so the binding
 * path below needs to know nothing about any of this.
 */
export function specialize(sql: string, params: Params): string {
  let active = "";
  for (const [, name] of sql.matchAll(OPTIONAL_FILTER)) {
    if (params[name] != null) active += name + ",";
  }
  if (!active) return sql;
  const key = active + sql;
  let out = specialCache.get(key);
  if (out === undefined) {
    out = sql.replace(OPTIONAL_FILTER, (whole, name: string, column: string) =>
      params[name] != null ? `${column} = :${name}` : whole,
    );
    specialCache.set(key, out);
  }
  return out;
}

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
/**
 * Why `watch` is calling back.
 *
 * `queued` / `done` come from the store's push channel and name what moved;
 * `poll` is the timer saying the watch cannot rule out a change. None of them
 * carries state — the row is the truth.
 */
export interface WatchSignal {
  reason: "queued" | "done" | "poll";
  /** The queue a task was queued on. Only on `queued`. */
  queue?: string;
  /** The task that reached a terminal status. Only on `done`. */
  taskId?: string;
}

export interface WatchOptions {
  /** Restrict `queued` signals to these queues. Unset watches every queue. */
  queues?: string[];
  /**
   * How often to signal in the absence of a push channel — and, where there is
   * one, how long a dropped listener can go unnoticed. The default trades a
   * dashboard's idle query rate against how stale it may look.
   */
  pollMs?: number;
}

/** See WatchOptions.pollMs. */
export const DEFAULT_WATCH_POLL_MS = 2_000;

export abstract class TaskStore {
  /** Set by useBackpressure; null means submit is ungated. */
  private gate: QueueDepthGate | null = null;

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
   * `tx`, but also handing `fn` the driver's own session so the CALLER can run
   * their statements in the same transaction as the protocol's.
   *
   * This is what lets a task's settlement and the rows that task produced commit
   * together. Without it the two are separate transactions and there is a window
   * where the work is durable but the task still reads as unfinished — a crash
   * there costs a full recomputation on retry, and for non-idempotent work costs
   * more than that.
   *
   * Optional, because the session type is the driver's, not the protocol's: a
   * store that has no session worth handing out simply does not implement it and
   * `completeIn` reports that. Postgres implements it; SQLite does not.
   */
  protected txWithSession?<T>(fn: (fetch: Fetch, session: unknown) => Promise<T>): Promise<T>;

  /**
   * Register for this store's push channel, if it has one; returns an
   * unsubscribe. A store without a push channel does not implement this, and
   * `watch` degrades to its timer alone.
   */
  protected subscribePush?(onSignal: (signal: WatchSignal) => void): () => void;

  /**
   * Tell a store with a push channel which queues this process will wait on, so
   * it can buffer their notifications and ignore everyone else's. Optional: a
   * store without a push channel has nothing to buffer.
   */
  protected registerWakeable?(queues: string[]): void;

  /**
   * Nudge the push channel back up if it has dropped. Called from `watch`'s
   * timer, which is the only thing keeping a client-side subscriber alive: a
   * process that never claims never calls claimWake, so without this a listener
   * that died once would never come back there.
   */
  protected warmPush?(): void;

  /**
   * Whether it is worth opening the claim transaction at all — the read-only
   * `claimable_probe`, which every dialect ships.
   *
   * Both dialects want it, for different reasons: SQLite so an idle worker never
   * takes its single write lock and idle workers stop serializing against each
   * other, Postgres so an empty poll costs one statement instead of a
   * transaction plus `recover_leases` plus one claim statement per
   * self-limiting name. Neither reason is dialect-specific enough to live in a
   * dialect: what differs is the SQL, which is where the protocol keeps dialect
   * differences already. A backend whose probe would cost more than the claim it
   * guards overrides this with `return true`.
   */
  protected async hasClaimableWork(params: Params): Promise<boolean> {
    const rows = await this.fetch("claimable_probe", params);
    return Boolean(rows[0]?.has_work);
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
   * Whether this store's driver hands JSON columns back as text.
   *
   * Declared by the store rather than sniffed per value, because for a JSON
   * *string* the two wire forms are indistinguishable: the text form of
   * `"hello"` and the decoded form of `"hello"` are both strings, and treating
   * the decoded one as text parses it twice — `"s3://…"` threw, `"42"` came back
   * as the number 42. See rowToTask.
   *
   * True is the SQLite answer and the safe default: its JSON columns are TEXT,
   * always. PostgresStore replaces it with what its driver actually does, which
   * it measures rather than assumes — an injected executor's driver is the
   * application's choice, not cairnq's.
   */
  protected jsonIsText = true;

  /** rowToTask, told this store's wire form. Every row in this class goes
   * through here so no call site has to remember to pass it. */
  private toTask(row: any): Task {
    return rowToTask(row, this.jsonIsText);
  }

  /**
   * An ownership-checked worker write (heartbeat/progress/succeed/complete/fail).
   * Each statement's WHERE pins worker_id + a live lease, so 0 rows back means
   * the lease was lost — every such write reports it the same way.
   */
  private async ownedWrite(name: string, taskId: string, params: Params): Promise<Task> {
    const rows = await this.fetch(name, params);
    if (!rows.length) throw new LostLease(taskId);
    return this.toTask(rows[0]);
  }

  // An instance method, unlike oneRef: mapping a Task reads this store's JSON
  // wire form, and a TaskRef has no JSON column to read.
  private one(rows: any[]): Task | null {
    return rows.length ? this.toTask(rows[0]) : null;
  }

  private static oneRef(rows: any[]): TaskRef | null {
    return rows.length ? rowToRef(rows[0]) : null;
  }

  // ------------------------------------------------------------- client side
  /**
   * Bound how deep a queue may get before `submit` blocks. Off unless set.
   *
   * It hangs here rather than on `CairnQ` because the store is the one choke
   * point every submit passes through — a handler spawning children via
   * `TaskContext.submit` is the shape most likely to outrun its workers, and
   * gating only the client would leave exactly that path unbounded.
   */
  useBackpressure(opts: BackpressureOptions): void {
    this.gate = new QueueDepthGate(this, opts);
  }

  async submit(input: SubmitInput): Promise<Task> {
    const id = newId("task");
    const ins: Params = {
      id,
      name: input.name,
      queue: input.queue ?? DEFAULT_QUEUE,
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
    // After validation and before the first write: bad arguments should fail
    // now, not after waiting out a full queue. Reads the resolved queue, so the
    // gate cannot throttle one queue while the row lands on another.
    if (this.gate) await this.gate.acquire(ins.queue as string);
    if (key === null) return this.toTask((await this.fetch("insert_task", ins))[0]);

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
          if (conflict === "reject") throw new AlreadyExists(key);
          if (reusable(conflict, current.status as TaskStatus)) return this.toTask(current);
          // The strategy declined the recorded task, so the key repoints to the
          // fresh one inserted below. Cancel only what is still live: a terminal
          // task has nothing to stop, and cancelling it would rewrite a settled
          // row (and hand a `canceled` back to whoever is waiting on it).
          if (!isTerminalStatus(current.status as TaskStatus)) {
            await fetch("cancel", { id: existing[0].task_id });
          }
        }
      }
      const row = (await fetch("insert_task", ins))[0];
      await fetch("upsert_key", { key, task_id: id });
      return this.toTask(row);
    });
  }

  async get(taskId: string): Promise<Task | null> {
    return this.one(await this.fetch("get", { id: taskId }));
  }

  async getByKey(key: string): Promise<Task | null> {
    return this.one(await this.fetch("get_by_key", { key }));
  }

  /** The wait loop's probe: id + status alone, so polling a task with a large
   * payload does not re-read and re-parse that payload on every beat. */
  async getStatus(taskId: string): Promise<TaskRef | null> {
    return TaskStore.oneRef(await this.fetch("get_status", { id: taskId }));
  }

  /** getStatus, following a key instead of an id — re-resolved per call, so a
   * `replace` moves the probe onto the new task. */
  async getStatusByKey(key: string): Promise<TaskRef | null> {
    return TaskStore.oneRef(await this.fetch("get_status_by_key", { key }));
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
    return rows.map((r) => this.toTask(r));
  }

  async cancel(taskId: string): Promise<Task | null> {
    return this.one(await this.fetch("cancel", { id: taskId }));
  }

  async retry(taskId: string, opts: { resetAttempt?: boolean } = {}): Promise<Task | null> {
    return this.one(
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
      return this.one(await fetch(name, { id: existing[0].task_id, ...params }));
    });
  }

  /**
   * Delete terminal tasks that completed more than `olderThanMs` ago and return
   * their ids. Nothing else removes rows, so a long-lived database needs this
   * called periodically. Bounded by `limit` to keep each sweep a short write;
   * call it in a loop until it returns fewer than `limit`.
   *
   * `queue` / `status` / `name` narrow the sweep, which is what makes tiered
   * retention expressible at all — see PurgeInput.
   */
  async purge(input: PurgeInput = {}): Promise<string[]> {
    validatePurgeInput(input);
    const rows = await this.fetch("purge", {
      older_than_ms: input.olderThanMs ?? 0,
      queue: input.queue ?? null,
      status: input.status ?? null,
      name: input.name ?? null,
      limit: input.limit ?? 1_000,
    });
    return rows.map((r) => r.id as string);
  }

  /**
   * Task counts per queue, keyed by status and zero-filled across all statuses —
   * `(await stats()).default.queued` is the backlog of a queue. A queue appears
   * only while it has rows; terminal tasks keep counting until `purge` removes
   * them.
   *
   * `queue` restricts the aggregate to one queue, which is also what stops the
   * caller paying for every other queue's rows: one installation carrying two
   * workloads is the coordination this project recommends, and the unfiltered
   * form reads the whole table. A named queue is always present in the result,
   * zero-filled if it has no rows at all — asking about a specific queue and
   * getting `undefined` back would make every caller write the same fallback.
   *
   * Filtered or not, this COUNTS, so it costs what it counts: a whole queue,
   * terminal rows included. Right for a dashboard, wrong on an interval — poll
   * `queueDepth`, which is bounded, and keep this for when the real numbers are
   * the point.
   */
  async stats(queue?: string): Promise<Record<string, Record<TaskStatus, number>>> {
    const zeros = (): Record<TaskStatus, number> =>
      Object.fromEntries(STATUSES.map((s) => [s, 0])) as Record<TaskStatus, number>;
    const out: Record<string, Record<TaskStatus, number>> = {};
    // Seed before the query, not after: a named queue with no rows returns no
    // rows to seed from, and that is exactly the case the promise is about.
    if (queue != null) out[queue] = zeros();
    for (const row of await this.fetch("stats", { queue: queue ?? null })) {
      const per = (out[row.queue] ??= zeros());
      per[row.status as TaskStatus] = Number(row.count);
    }
    return out;
  }

  /**
   * Call `onSignal` when the tasks on `queues` may have changed — something was
   * queued, or something finished.
   *
   * This is notify-ACCELERATED POLLING, not an event log, and the difference is
   * the whole contract. Where a push channel is available (Postgres LISTEN) an
   * idle watch costs nothing and a signal arrives within milliseconds of the
   * event. Where it is not — a transaction-mode pooler refuses LISTEN, SQLite has
   * no channel at all — the timer alone still delivers `poll` signals, so a
   * consumer that re-reads on every signal is correct in both cases and merely
   * less prompt in one.
   *
   * What it will NOT do is promise that a signal means something happened, or
   * that every event produces its own signal. Treat a signal as "re-read now"
   * and take the truth from `stats()` / `list()` / `get()`, which is where it
   * lives. `reason` is a hint for reading less: a `done` signal names the task,
   * so a dashboard can refresh that row instead of the list.
   *
   * Returns an unsubscribe. The timer is unref'd — watching does not hold a
   * process open.
   */
  watch(opts: WatchOptions, onSignal: (signal: WatchSignal) => void): () => void {
    const pollMs = Math.max(1, opts.pollMs ?? DEFAULT_WATCH_POLL_MS);
    const queues = opts.queues ?? null;
    let live = true;
    const emit = (signal: WatchSignal): void => {
      // A signal delivered after unsubscribe would have the consumer re-reading
      // a store it has stopped caring about, possibly a closed one.
      if (live) onSignal(signal);
    };
    const unsubscribe = this.subscribePush?.((signal) => {
      // A queued signal names its queue, so a watch scoped to some queues can
      // drop the rest. A done signal names only the task — which queue it was on
      // is not in the notification, so it is never filtered out.
      if (signal.reason === "queued" && queues && signal.queue && !queues.includes(signal.queue)) {
        return;
      }
      emit(signal);
    });
    const timer = setInterval(() => {
      this.warmPush?.();
      // Guarded for the same reason PostgresStore guards its push fan-out: a
      // consumer that throws must not take the timer down with it. Losing the
      // timer would silently retire the fallback that makes watch correct
      // where there is no push channel at all.
      try {
        emit({ reason: "poll" });
      } catch {
        // The consumer's problem, not the watch's.
      }
    }, pollMs);
    timer.unref?.();
    return () => {
      live = false;
      unsubscribe?.();
      clearInterval(timer);
    };
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
    const names = input.names ?? null;
    const claimed = await this.claimSession(
      { queues: input.queues, workerId: input.workerId, leaseMs: input.leaseMs, names },
      (claim) => claim(names, input.limit ?? 1),
    );
    return claimed ?? [];
  }

  /**
   * Open one claim transaction and let the caller draw from it repeatedly.
   *
   * The transaction is what has to live here: the read-only probe that keeps an
   * idle worker off SQLite's single write lock, the `recover_leases` whose
   * reclaimed leases must be visible to the claims that follow and to nobody in
   * between, and the write lock itself. *What* gets claimed under it is the
   * caller's business — a worker drawing a separate quota per task name is
   * scheduling policy, and this layer has no vocabulary for the "handler call"
   * that policy is denominated in. It knows queues, names, limits and rows.
   *
   * `plan` is handed a `claim(names, limit)` it may call any number of times,
   * each a separate statement under the same lock and the same recovery, and
   * each free to size itself from what the previous one returned. That feedback
   * is the reason this is a callback rather than a list of quotas: a caller
   * dividing a budget up front has to guess, and every share handed to a name
   * with nothing queued is a slot left idle until the next poll.
   *
   * `plan` runs with the write lock held, so it must await nothing but that
   * callback.
   *
   * `names` is the union `plan` might ask for, and it filters the probe's
   * queued-work arm. Lease recovery is deliberately NOT filtered by it — nor is
   * the probe's expired-lease arm — because reclaiming a dead worker's task is
   * every worker's job, whatever names it happens to handle. Returns undefined
   * when the probe finds nothing claimable, in which case `plan` never runs and
   * no transaction is opened.
   */
  async claimSession<T>(
    input: { queues: string[]; workerId: string; leaseMs?: number; names: string[] | null },
    plan: (claim: (names: string[] | null, limit: number) => Promise<Task[]>) => Promise<T>,
  ): Promise<T | undefined> {
    // A list-valued filter cannot be read in claim order, so the planner sorts
    // every claimable row to take LIMIT of them and the claim's cost grows with
    // the backlog while it holds the transaction. Both filters therefore have an
    // equality form, picked per draw: one queue is the common deployment, and one
    // name is every per-name quota. See claim_one_queue.sql and claim_one_name.sql.
    const oneQueue = input.queues.length === 1;
    const base: Params = {
      queues: input.queues,
      queue: oneQueue ? input.queues[0] : null,
      names: input.names,
      name: null,
      worker_id: input.workerId,
      lease_ms: input.leaseMs ?? 30_000,
      limit: 1,
      lease_expired_error: LEASE_EXPIRED_ERROR_JSON,
    };
    // Before the probe: a push-channel store has to know what this caller waits
    // on from its FIRST claim, not from its first empty poll.
    this.registerWakeable?.(input.queues);
    if (!(await this.hasClaimableWork(base))) return undefined;
    return this.tx(async (fetch) => {
      await fetch("recover_leases", base);
      return plan(async (names, limit) => {
        // A draw asking for nothing, or filtered to no names, claims nothing —
        // answer it here rather than spending a statement to learn that.
        if (limit <= 0 || names?.length === 0) return [];
        const oneName = names?.length === 1;
        const statement = oneName
          ? oneQueue
            ? "claim_one_queue_one_name"
            : "claim_one_name"
          : oneQueue
            ? "claim_one_queue"
            : "claim";
        const rows = await fetch(statement, {
          ...base,
          names,
          name: oneName ? names![0] : null,
          limit,
        });
        return rows.map((r) => this.toTask(r));
      });
    });
  }

  async heartbeat(input: { taskId: string; workerId: string; leaseMs?: number }): Promise<Task> {
    return this.ownedWrite("heartbeat", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      lease_ms: input.leaseMs ?? 30_000,
    });
  }

  /**
   * Renew several leases in one statement. Returns `taskId -> cancel requested`
   * for the tasks this worker still holds.
   *
   * Deliberately not an ownedWrite: ownership is per task here, so there is no
   * single answer to "did it work". A task **absent** from the result lost its
   * lease, and the caller decides what that means for that one task rather than
   * failing the whole beat.
   *
   * It returns flags rather than Tasks because nothing downstream needs a task:
   * the caller renews leases and observes cancellation, and whole rows would drag
   * every payload back on every beat for the life of the call.
   */
  async heartbeatBatch(input: {
    taskIds: string[];
    workerId: string;
    leaseMs?: number;
  }): Promise<Map<string, boolean>> {
    if (!input.taskIds.length) return new Map();
    const rows = await this.fetch("heartbeat_batch", {
      ids: input.taskIds,
      worker_id: input.workerId,
      lease_ms: input.leaseMs ?? 30_000,
    });
    return new Map(rows.map((r) => [r.id as string, r.cancel_requested_at_ms != null]));
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

  /**
   * `complete`, with the caller's own writes committed in the same transaction.
   *
   * `fn` runs first and whatever it returns becomes the task's result; the
   * settlement is the last statement in the transaction. So a lost lease — the
   * settlement matching no row — rolls the caller's writes back with it, and
   * there is no ordering in which the work is recorded but the task is not.
   *
   * The settlement runs LAST rather than checking ownership up front on purpose:
   * the ownership predicate lives in the protocol's complete.sql, and a
   * fail-fast pre-check here would be a second copy of it, free to drift. The
   * cost of that choice is that a doomed attempt does its work before finding
   * out, which is the rare path.
   *
   * `fn` must be replayable for the same reason `tx`'s callback must be.
   */
  async completeIn<S, T>(
    input: { taskId: string; workerId: string },
    fn: (session: S) => Promise<T>,
  ): Promise<{ task: Task; value: T }> {
    if (!this.txWithSession) {
      throw new UnsupportedBackend(
        "this store cannot share a transaction with the caller — " +
          "completeIn requires a Postgres store (see PgExecutor)",
      );
    }
    return this.txWithSession(async (fetch, session) => {
      const value = await fn(session as S);
      const rows = await fetch("complete", {
        id: input.taskId,
        worker_id: input.workerId,
        result: value == null ? null : dumpJson(value),
      });
      // Rolls back `fn`'s writes along with the settlement that did not land.
      if (!rows.length) throw new LostLease(input.taskId);
      return { task: this.toTask(rows[0]), value };
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
