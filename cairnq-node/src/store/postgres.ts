import { SchemaMismatch } from "../errors.js";
import { loadMigrations, loadStatements } from "../sql.js";
import {
  checkProtocolVersion,
  COMMENT,
  type Fetch,
  NAMED,
  type Params,
  statementParams,
  TaskStore,
  type WatchSignal,
} from "./base.js";
import { ListenUnavailable, type PgExecutor, type PgSession } from "./pg-executor.js";
import { createPoolExecutor } from "./pg-pool.js";

// Notification channels, emitted by the 0003_notify trigger.
const QUEUED_CHANNEL = "cairnq_queued";
const DONE_CHANNEL = "cairnq_done";

// Backoff between attempts to (re)establish the LISTEN subscription after a
// transient failure. Doubles per failure up to the cap; polling covers the gap.
const LISTENER_RETRY_MS = 1_000;
const LISTENER_RETRY_MAX_MS = 30_000;

const translated = new Map<string, { text: string; order: readonly string[] }>();

/**
 * The rewritten SQL and the order its `$n` slots must be filled in.
 *
 * Translates the protocol's named-parameter SQL (`:name`) into Postgres positional
 * placeholders (`$1`), collapsing each DISTINCT name to ONE slot — statements reuse
 * a name across CASE branches / IS NULL guards (e.g. list.sql). Which names count
 * as parameters is `statementParams`' decision, shared with the SQLite binding path
 * so the two can't disagree about, say, a `::type` cast.
 *
 * Memoized on the statement text, which is loaded once and never varies: this runs
 * on every query, including the worker's poll loop, for a result that cannot have
 * changed. Exported for unit testing.
 */
export function positionalStatement(sql: string): { text: string; order: readonly string[] } {
  let entry = translated.get(sql);
  if (!entry) {
    const order = statementParams(sql);
    const slot = new Map(order.map((name, i) => [name, i + 1])); // 1-based $n
    const text = sql
      .replace(COMMENT, "")
      .replace(NAMED, (_m, name: string) => `$${slot.get(name)}`);
    entry = { text, order };
    translated.set(sql, entry);
  }
  return entry;
}

/**
 * The statement's rewritten text plus this call's values, in slot order. Names the
 * statement does not use are simply not bound, so callers may pass a superset.
 */
export function toPositional(
  sql: string,
  params: Params,
): { text: string; values: unknown[] } {
  const { text, order } = positionalStatement(sql);
  return { text, values: order.map((n) => params[n]) };
}

/**
 * PostgresStore — the Postgres dialect of the shared cairnq-protocol SQL.
 *
 * Everything protocol-shaped lives in TaskStore; this file is only what Postgres
 * does differently: `:name` -> `$n` translation, the migration ledger, LISTEN
 * policy, and time taken from the DB clock (`now()`) instead of from the SDK,
 * which is what makes this backend multi-host — unlike SQLite it coordinates API
 * and worker processes across machines, with no shared clock to agree on. claim
 * uses FOR UPDATE SKIP LOCKED and needs no claimable_probe, because PG readers
 * don't block writers. JSON columns are jsonb (bound as JSON text, read back as
 * objects by rowToTask).
 *
 * What it deliberately does NOT own is the connection. Given a DSN it builds a
 * `pg` pool (see pg-pool.ts); given a PgExecutor it runs inside the caller's
 * session instead — no second driver, no second pool, and the caller's writes and
 * cairnq's can share one transaction.
 */
export class PostgresStore extends TaskStore {
  private readonly dsn: string | null;
  /** The caller's executor, if one was injected — never closed by this store. */
  private readonly provided: PgExecutor | null;
  /** Set once migrations have run and the protocol version checked out. */
  private executor: PgExecutor | null = null;
  private connecting: Promise<void> | null = null;
  private readonly statements: Record<string, string>;

  // ------------------------------------------------------- LISTEN/NOTIFY state
  // The executor subscribes one dedicated connection to both channels (see
  // 0003_notify.sql and the claimWake/taskDoneWake contract on TaskStore).
  // Failure to establish or keep it silently degrades the store to the base
  // class's plain polling. Held as the unsubscribe function, not the connection:
  // whose connection it is, is the executor's business.
  private listener: (() => void) | null = null;
  private listenerConnecting: Promise<void> | null = null;
  /** LISTEN is off for good: the store was closed, the executor does not support
   * it, or the server refused it (e.g. a transaction-mode pooler) —
   * deterministic, so retrying would fail the same way every time. */
  private listenerUnavailable = false;
  /** A failure to even connect is transient (network blip, server restarting):
   * retry, but not before this time, backing off so a down server is not
   * hammered from the poll loop. */
  private listenerRetryAt = 0;
  private listenerBackoffMs = LISTENER_RETRY_MS;
  /** Queues notified while nobody was waiting; consumed by the next claimWake
   * so a wake that lands between polls is not lost. */
  private readonly pendingQueues = new Set<string>();
  /** Wake callbacks by key: "queued" (broadcast) or "done:<task id>". */
  private readonly waiters = new Map<string, Set<() => void>>();
  /** watch() subscribers. Separate from `waiters`: a waiter is one-shot and
   * consumes the notification, a subscriber is standing and only observes. */
  private readonly subscribers = new Set<(signal: WatchSignal) => void>();

  /**
   * `source` is either a libpq connection string — this store then owns a `pg`
   * pool and requires the optional `pg` package — or a PgExecutor the caller
   * already has, which this store uses and never closes.
   */
  constructor(
    source: string | PgExecutor,
    private readonly opts: { max?: number; schema?: string } = {},
  ) {
    super();
    this.dsn = typeof source === "string" ? source : null;
    this.provided = typeof source === "string" ? null : source;
    this.statements = loadStatements("postgres");
  }

  async connect(): Promise<void> {
    await this.ensure();
  }

  async close(): Promise<void> {
    this.listenerUnavailable = true; // no revival after close
    this.dropListener();
    const executor = this.executor;
    this.executor = null;
    this.connecting = null;
    // An injected executor belongs to the caller, whose other work would not
    // survive cairnq closing it.
    if (executor && !this.provided) await executor.close();
  }

  private async ensure(): Promise<void> {
    if (this.executor) return;
    // Cache the in-flight connect so concurrent calls share one executor. On
    // failure, clear it so a later call retries instead of re-awaiting a
    // rejected promise.
    if (!this.connecting) {
      this.connecting = this.doConnect().catch((e) => {
        this.connecting = null;
        throw e;
      });
    }
    await this.connecting;
  }

  private async doConnect(): Promise<void> {
    const executor =
      this.provided ??
      (await createPoolExecutor(this.dsn!, { max: this.opts.max, schema: this.opts.schema }));
    try {
      // Before migrations, which would otherwise create the very installation
      // this is trying to warn about.
      await this.checkSchema(executor);
      await this.applyMigrations(executor);
      checkProtocolVersion(await this.readProtocolVersion(executor));
    } catch (e) {
      // Never leak an executor we created; never close one we were handed.
      if (!this.provided) await executor.close().catch(() => {});
      throw e;
    }
    this.executor = executor; // publish only a fully-migrated, version-checked executor
    // Warm the LISTEN subscription in the background so the first idle sleep is
    // already wakeable. Fire-and-forget: failure just means polling.
    this.listenerReady();
  }

  /**
   * Refuse a connection pointed somewhere other than the deployment's cairnq.
   *
   * Two shapes, because `schema` means "the schema cairnq's tables live in" and
   * cairnq can either arrange that (it built the connection) or only check it
   * (the caller's executor did):
   *
   * - `schema` configured -> assert the connection actually resolves there. On
   *   the DSN path this is a cheap self-check; on an injected executor it is the
   *   only way to state the expectation at all.
   * - `schema` not configured -> the dangerous case is being about to create a
   *   SECOND installation while one already exists elsewhere in this database,
   *   which is exactly what a mismatched pair of SDKs does. Joining an existing
   *   installation is fine no matter what else is around, so the check is
   *   deliberately narrow: it fires only when this schema has no cairnq and some
   *   other schema does.
   *
   * That narrowness is what keeps it from crying wolf. Two applications each
   * running their own cairnq in their own schema are legitimate; the second one
   * to be set up trips this once, and saying `schema` explicitly — which such a
   * deployment should be doing anyway — is both the fix and the confirmation.
   */
  private async checkSchema(executor: PgExecutor): Promise<void> {
    // One row per installation; the statement's LEFT JOIN guarantees at least
    // one, so current_schema is readable even where cairnq lives nowhere yet.
    const rows = await executor.query(this.statements.installations, []);
    const current = (rows[0]?.current_schema as string | null) ?? null;
    const installations = rows
      .map((r) => r.schema as string | null)
      .filter((s): s is string => s != null);
    const wanted = this.opts.schema;

    if (wanted != null) {
      if (current !== wanted) {
        throw new SchemaMismatch(
          `cairnq is configured for schema ${JSON.stringify(wanted)} but this ` +
            `connection resolves to ${JSON.stringify(current)} — check the ` +
            `connection's search_path`,
        );
      }
      return;
    }
    // A search_path naming nothing that exists: there is no "here" to compare
    // against, and the migrations are about to fail with a clearer message.
    if (current === null) return;
    if (installations.length === 0 || installations.includes(current)) return;

    throw new SchemaMismatch(
      `cairnq tables already exist in schema ${installations.map((s) => JSON.stringify(s)).join(", ")} ` +
        `of this database, but this connection resolves to ${JSON.stringify(current)}, where there are ` +
        `none. Connecting would create a second, parallel installation that the other one can never see ` +
        `— an API and a worker split this way agree about everything except where, and no task crosses. ` +
        `Point this process at the same schema (\`schema\` option, or \`options=-c search_path=...\` in the ` +
        `DSN), or pass \`schema\` explicitly to confirm a separate installation is what you meant.`,
    );
  }

  private async applyMigrations(executor: PgExecutor): Promise<void> {
    await executor.exec(
      "create table if not exists cairnq_migrations " +
        "(name text primary key, applied_at_ms bigint not null)",
    );
    for (const { name, sql } of loadMigrations("postgres")) {
      // Check and apply inside one transaction, with the table lock taken up
      // front: two processes cold-starting together would otherwise both see a
      // migration as unapplied and both run it.
      await executor.tx(async (s) => {
        await s.exec("lock table cairnq_migrations in exclusive mode");
        const applied = await s.query("select 1 from cairnq_migrations where name = $1", [name]);
        if (applied.length === 0) {
          await s.exec(sql); // multi-statement DDL (simple-query, no params)
          await s.query(
            "insert into cairnq_migrations (name, applied_at_ms) values " +
              "($1, (extract(epoch from now()) * 1000)::bigint)",
            [name],
          );
        }
      });
    }
  }

  // Takes an explicit session: during doConnect the executor is not published
  // yet, so this cannot go through fetch(). The statement binds nothing.
  private async readProtocolVersion(s: PgSession): Promise<number> {
    const rows = await s.query(this.statements.protocol_version, []);
    return rows.length ? Number(rows[0].value) : 0;
  }

  async protocolVersion(): Promise<number> {
    await this.ensure();
    return this.readProtocolVersion(this.executor!);
  }

  // ------------------------------------------------------------ wake channel
  async claimWake(queues: string[], timeoutMs: number): Promise<void> {
    if (!this.listenerReady()) return super.claimWake(queues, timeoutMs);
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      // Consume every pending notification for our queues; any hit wakes us.
      let hit = false;
      for (const q of queues) if (this.pendingQueues.delete(q)) hit = true;
      if (hit) return;
      const remaining = deadline - Date.now();
      if (remaining <= 0) return;
      // Broadcast wake: another queue's notification loops us back to waiting
      // with the remaining budget instead of waking the worker for nothing.
      await this.wakeOn("queued", remaining);
    }
  }

  protected subscribePush(onSignal: (signal: WatchSignal) => void): () => void {
    this.subscribers.add(onSignal);
    this.listenerReady(); // an API-side watcher is often the only thing asking
    return () => void this.subscribers.delete(onSignal);
  }

  protected warmPush(): void {
    if (this.subscribers.size > 0) this.listenerReady();
  }

  taskDoneWake(taskId: string, timeoutMs: number): Promise<void> {
    if (!this.listenerReady()) return super.taskDoneWake(taskId, timeoutMs);
    return this.wakeOn(`done:${taskId}`, timeoutMs);
  }

  /** A promise resolving on notification-or-timeout, deregistering either way.
   * The timer is unref'd: while a listener exists its socket keeps the process
   * alive, and dropListener wakes every waiter the moment it goes away. */
  private wakeOn(key: string, timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      let set = this.waiters.get(key);
      if (!set) this.waiters.set(key, (set = new Set()));
      const peers = set;
      const waiter = () => {
        clearTimeout(timer);
        peers.delete(waiter);
        if (peers.size === 0) this.waiters.delete(key);
        resolve();
      };
      const timer = setTimeout(waiter, timeoutMs);
      timer.unref?.();
      peers.add(waiter);
    });
  }

  /** True once the LISTEN subscription is up; starts establishing it otherwise
   * (respecting the transient-failure backoff). Callers fall back to plain
   * polling until it is ready (or forever, if it can't be established) —
   * correctness never depends on it. */
  private listenerReady(): boolean {
    if (this.listener) return true;
    if (!this.listenerUnavailable && !this.listenerConnecting && Date.now() >= this.listenerRetryAt) {
      this.listenerConnecting = this.startListener();
    }
    return false;
  }

  private async startListener(): Promise<void> {
    try {
      const executor = this.executor ?? this.provided;
      // Not connected yet: transient by definition — doConnect calls back in.
      if (!executor) return;
      if (!executor.listen) {
        this.listenerUnavailable = true; // this executor will never push
        return;
      }
      let stop: () => void;
      try {
        stop = await executor.listen(
          [QUEUED_CHANNEL, DONE_CHANNEL],
          (channel, payload) => this.onNotification(channel, payload),
          // A dropped listener degrades to polling; the next wake reconnects.
          () => this.dropListener(),
        );
      } catch (e) {
        if (e instanceof ListenUnavailable) {
          // Deterministic (e.g. a transaction-mode pooler): off for good rather
          // than a reconnect loop that cannot succeed. Polling covers it.
          this.listenerUnavailable = true;
          return;
        }
        // Could not establish it — transient. Schedule a backed-off retry;
        // polling covers the gap.
        this.listenerRetryAt = Date.now() + this.listenerBackoffMs;
        this.listenerBackoffMs = Math.min(LISTENER_RETRY_MAX_MS, this.listenerBackoffMs * 2);
        return;
      }
      if (this.listenerUnavailable) {
        stop(); // closed while we were subscribing
        return;
      }
      this.listener = stop;
      this.listenerBackoffMs = LISTENER_RETRY_MS;
    } catch {
      // Anything unexpected (e.g. `pg` failed to load): off for good rather
      // than a retry loop that cannot succeed.
      this.listenerUnavailable = true;
    } finally {
      this.listenerConnecting = null;
    }
  }

  private onNotification(channel: string, payload: string | undefined): void {
    let key: string;
    if (channel === QUEUED_CHANNEL && payload) {
      this.pendingQueues.add(payload);
      key = "queued";
      this.publish({ reason: "queued", queue: payload });
    } else if (channel === DONE_CHANNEL && payload) {
      key = `done:${payload}`;
      this.publish({ reason: "done", taskId: payload });
    } else {
      return;
    }
    const set = this.waiters.get(key);
    // A waiter only deletes itself, which Set iteration tolerates — no copy.
    if (set) for (const w of set) w();
  }

  /** Hand a notification to every watch() subscriber. A throwing subscriber is
   * its own problem: it must not cost the others their signal, nor take down the
   * listener connection that delivered it. */
  private publish(signal: WatchSignal): void {
    for (const s of [...this.subscribers]) {
      try {
        s(signal);
      } catch {
        // Deliberately swallowed — see above.
      }
    }
  }

  private dropListener(): void {
    const stop = this.listener;
    this.listener = null;
    stop?.();
    // Release everyone promptly; their fallback poll takes over.
    for (const set of [...this.waiters.values()]) for (const w of [...set]) w();
  }

  // ------------------------------------------------------------ dialect seam
  protected async fetch(name: string, params: Params): Promise<any[]> {
    await this.ensure();
    const { text, values } = toPositional(this.statements[name], params);
    return this.executor!.query(text, values);
  }

  protected async tx<T>(fn: (fetch: Fetch) => Promise<T>): Promise<T> {
    await this.ensure();
    return this.executor!.tx((s) => fn(this.boundFetch(s)));
  }

  protected async txWithSession<T>(
    fn: (fetch: Fetch, session: PgSession) => Promise<T>,
  ): Promise<T> {
    await this.ensure();
    return this.executor!.tx((s) => fn(this.boundFetch(s), s));
  }

  /** A Fetch that runs the protocol's statements on one particular session. */
  private boundFetch(s: PgSession): Fetch {
    return async (name, params) => {
      const { text, values } = toPositional(this.statements[name], params);
      return s.query(text, values);
    };
  }
}
