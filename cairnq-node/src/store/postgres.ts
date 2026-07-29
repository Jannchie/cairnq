import type * as PG from "pg";

import { loadMigrations, loadStatements } from "../sql.js";
import {
  checkProtocolVersion,
  COMMENT,
  type Fetch,
  NAMED,
  type Params,
  statementParams,
  TaskStore,
} from "./base.js";

// `pg` is an optional dependency: the SDK is SQLite-first, so it's loaded lazily
// the first time a PostgresStore connects. Absent -> a clear install hint.
let pgModule: typeof import("pg") | null = null;
async function loadPg(): Promise<typeof import("pg")> {
  if (pgModule) return pgModule;
  let mod: { default?: typeof import("pg") } & typeof import("pg");
  try {
    mod = (await import("pg")) as never;
  } catch {
    throw new Error("PostgresStore requires the 'pg' package — install it (e.g. `npm i pg`)");
  }
  const pg = (mod.default ?? mod) as typeof import("pg");
  // Postgres returns bigint (int8, OID 20) as a string to avoid precision loss.
  // Every cairnq bigint is an epoch-ms or a counter, all within Number's safe
  // integer range, so parse to number once (globally) to match the Task model
  // (*_ms typed as number, same as the SQLite SDK). Set before any query runs.
  pg.types.setTypeParser(pg.types.builtins.INT8, (v: string) => (v == null ? null : Number(v)));
  pgModule = pg;
  return pg;
}

/**
 * Roll back on the way out of a failed transaction, without letting the rollback
 * become the error the caller sees. A dropped connection fails both the statement
 * and the rollback, and it is the first one that says what went wrong.
 */
async function rollbackQuietly(client: PG.PoolClient): Promise<void> {
  try {
    await client.query("rollback");
  } catch {
    // Already rolled back, or the connection is gone. Either way the original
    // error is the one worth propagating.
  }
}

// Notification channels, emitted by the 0003_notify trigger.
const QUEUED_CHANNEL = "cairnq_queued";
const DONE_CHANNEL = "cairnq_done";

// Backoff between attempts to (re)connect the LISTEN connection after a
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
 * does differently: a `pg` Pool, `:name` -> `$n` translation, and time taken from
 * the DB clock (`now()`) instead of from the SDK, which is what makes this backend
 * multi-host — unlike SQLite it coordinates API and worker processes across
 * machines, with no shared clock to agree on. claim uses FOR UPDATE SKIP LOCKED
 * and needs no claimable_probe, because PG readers don't block writers. JSON
 * columns are jsonb (bound as JSON text, read back as objects by rowToTask).
 */
export class PostgresStore extends TaskStore {
  private pool: PG.Pool | null = null;
  private connecting: Promise<void> | null = null;
  private readonly statements: Record<string, string>;

  // ------------------------------------------------------- LISTEN/NOTIFY state
  // One dedicated connection LISTENs on both channels (see 0003_notify.sql and
  // the claimWake/taskDoneWake contract on TaskStore). Failure to establish or
  // keep it silently degrades the store to the base class's plain polling.
  private listener: PG.Client | null = null;
  private listenerConnecting: Promise<void> | null = null;
  /** LISTEN is off for good: the store was closed, or the server accepted a
   * connection but refused LISTEN (e.g. a transaction-mode pooler) —
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

  constructor(
    private readonly dsn: string,
    private readonly opts: { max?: number } = {},
  ) {
    super();
    this.statements = loadStatements("postgres");
  }

  async connect(): Promise<void> {
    await this.ensure();
  }

  async close(): Promise<void> {
    this.listenerUnavailable = true; // no revival after close
    this.dropListener();
    if (this.pool) {
      const p = this.pool;
      this.pool = null;
      this.connecting = null;
      await p.end();
    }
  }

  private async ensure(): Promise<void> {
    if (this.pool) return;
    // Cache the in-flight connect so concurrent calls share one pool. On failure,
    // clear it so a later call retries instead of re-awaiting a rejected promise.
    if (!this.connecting) {
      this.connecting = this.doConnect().catch((e) => {
        this.connecting = null;
        throw e;
      });
    }
    await this.connecting;
  }

  private async doConnect(): Promise<void> {
    const pg = await loadPg();
    const pool = new pg.Pool({ connectionString: this.dsn, max: this.opts.max });
    try {
      const client = await pool.connect();
      try {
        await this.applyMigrations(client);
        checkProtocolVersion(await this.readProtocolVersion(client));
      } finally {
        client.release();
      }
    } catch (e) {
      await pool.end(); // never leak a pool when connect fails
      throw e;
    }
    this.pool = pool; // publish only a fully-migrated, version-checked pool
    // Warm the LISTEN connection in the background so the first idle sleep is
    // already wakeable. Fire-and-forget: failure just means polling.
    this.listenerReady();
  }

  private async applyMigrations(client: PG.PoolClient): Promise<void> {
    await client.query(
      "create table if not exists cairnq_migrations " +
        "(name text primary key, applied_at_ms bigint not null)",
    );
    for (const { name, sql } of loadMigrations("postgres")) {
      // Check and apply inside one transaction, with the table lock taken up
      // front: two processes cold-starting together would otherwise both see a
      // migration as unapplied and both run it.
      try {
        await client.query("begin");
        await client.query("lock table cairnq_migrations in exclusive mode");
        const applied = await client.query("select 1 from cairnq_migrations where name = $1", [
          name,
        ]);
        if (applied.rowCount === 0) {
          await client.query(sql); // multi-statement DDL (simple-query, no params)
          await client.query(
            "insert into cairnq_migrations (name, applied_at_ms) values " +
              "($1, (extract(epoch from now()) * 1000)::bigint)",
            [name],
          );
        }
        await client.query("commit");
      } catch (e) {
        await rollbackQuietly(client);
        throw e;
      }
    }
  }

  // Takes an explicit client: during doConnect the pool is not published yet, so
  // this cannot go through fetch(). The statement binds nothing, so it runs as-is.
  private async readProtocolVersion(client: PG.PoolClient): Promise<number> {
    const res = await client.query(this.statements.protocol_version);
    return res.rows.length ? Number(res.rows[0].value) : 0;
  }

  async protocolVersion(): Promise<number> {
    await this.ensure();
    const client = await this.pool!.connect();
    try {
      return await this.readProtocolVersion(client);
    } finally {
      client.release();
    }
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

  /** True once the LISTEN connection is up; starts connecting it otherwise
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
      const pg = await loadPg();
      // Built from the raw DSN: if `opts` ever grows connection-level settings
      // (ssl, application_name), the listener must receive them too.
      const client = new pg.Client({ connectionString: this.dsn });
      try {
        await client.connect();
      } catch {
        // Could not even connect — transient. Schedule a backed-off retry;
        // polling covers the gap.
        this.listenerRetryAt = Date.now() + this.listenerBackoffMs;
        this.listenerBackoffMs = Math.min(LISTENER_RETRY_MAX_MS, this.listenerBackoffMs * 2);
        return;
      }
      client.on("notification", (msg) => this.onNotification(msg.channel, msg.payload));
      // A dropped listener degrades to polling; the next wake call reconnects.
      client.on("error", () => this.dropListener());
      try {
        await client.query(`listen ${QUEUED_CHANNEL}; listen ${DONE_CHANNEL}`);
      } catch {
        // Connected, but LISTEN was refused (e.g. a transaction-mode pooler) —
        // deterministic, so off for good. Polling covers it.
        this.listenerUnavailable = true;
        void client.end().catch(() => {});
        return;
      }
      if (this.listenerUnavailable) {
        void client.end().catch(() => {}); // closed while we were connecting
        return;
      }
      this.listener = client;
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
    } else if (channel === DONE_CHANNEL && payload) {
      key = `done:${payload}`;
    } else {
      return;
    }
    const set = this.waiters.get(key);
    // A waiter only deletes itself, which Set iteration tolerates — no copy.
    if (set) for (const w of set) w();
  }

  private dropListener(): void {
    const client = this.listener;
    this.listener = null;
    if (client) void client.end().catch(() => {});
    // Release everyone promptly; their fallback poll takes over.
    for (const set of [...this.waiters.values()]) for (const w of [...set]) w();
  }

  // ------------------------------------------------------------ dialect seam
  protected async fetch(name: string, params: Params): Promise<any[]> {
    await this.ensure();
    const { text, values } = toPositional(this.statements[name], params);
    return (await this.pool!.query(text, values)).rows;
  }

  protected async tx<T>(fn: (fetch: Fetch) => Promise<T>): Promise<T> {
    await this.ensure();
    const client = await this.pool!.connect();
    try {
      await client.query("begin");
      const out = await fn(async (name, params) => {
        const { text, values } = toPositional(this.statements[name], params);
        return (await client.query(text, values)).rows;
      });
      await client.query("commit");
      return out;
    } catch (e) {
      await rollbackQuietly(client);
      throw e;
    } finally {
      client.release();
    }
  }
}
