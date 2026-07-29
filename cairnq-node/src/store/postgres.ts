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
  // One dedicated connection LISTENs on both channels (see 0003_notify.sql).
  // Notifications are an accelerator: every wake path keeps its poll fallback,
  // so when this connection can't be established (e.g. a pooler without LISTEN
  // support) or drops, the store silently degrades to plain polling.
  private listener: PG.Client | null = null;
  private listenerState: "none" | "connecting" | "ready" | "closed" = "none";
  /** A cairnq_queued notification that arrived with nobody waiting; consumed by
   * the next claimWake so a wake between polls is not lost. */
  private wakePending = false;
  private readonly queuedWaiters = new Set<() => void>();
  private readonly doneWaiters = new Map<string, Set<() => void>>();

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
    this.listenerState = "closed"; // no revival after close
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
  // this cannot go through fetch().
  private async readProtocolVersion(client: PG.PoolClient): Promise<number> {
    const { text } = toPositional(this.statements.protocol_version, {});
    const res = await client.query(text);
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
  claimWake(timeoutMs: number): Promise<void> | null {
    if (!this.listenerReady()) return null;
    if (this.wakePending) {
      this.wakePending = false;
      return Promise.resolve();
    }
    return this.waiterWithTimeout(this.queuedWaiters, timeoutMs);
  }

  taskDoneWake(taskId: string, timeoutMs: number): Promise<void> | null {
    if (!this.listenerReady()) return null;
    let set = this.doneWaiters.get(taskId);
    if (!set) this.doneWaiters.set(taskId, (set = new Set()));
    const perTask = set;
    return this.waiterWithTimeout(perTask, timeoutMs).finally(() => {
      if (perTask.size === 0) this.doneWaiters.delete(taskId);
    });
  }

  /** A promise resolving on notification-or-timeout, deregistering either way. */
  private waiterWithTimeout(set: Set<() => void>, timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      const waiter = () => {
        clearTimeout(timer);
        set.delete(waiter);
        resolve();
      };
      const timer = setTimeout(waiter, timeoutMs);
      set.add(waiter);
    });
  }

  /** True once the LISTEN connection is up; kicks off connecting it otherwise.
   * Callers fall back to plain polling until it is ready (or forever, if it
   * can't be established) — correctness never depends on it. */
  private listenerReady(): boolean {
    if (this.listenerState === "ready") return true;
    if (this.listenerState === "none") {
      this.listenerState = "connecting";
      void this.startListener();
    }
    return false;
  }

  private async startListener(): Promise<void> {
    try {
      const pg = await loadPg();
      const client = new pg.Client({ connectionString: this.dsn });
      await client.connect();
      client.on("notification", (msg) => this.onNotification(msg.channel, msg.payload));
      // A dropped listener degrades to polling; the next wake call reconnects.
      client.on("error", () => {
        if (this.listenerState !== "closed") this.listenerState = "none";
        this.dropListener();
      });
      await client.query("listen cairnq_queued; listen cairnq_done");
      if ((this.listenerState as string) === "closed") {
        await client.end();
        return;
      }
      this.listener = client;
      this.listenerState = "ready";
    } catch {
      // Can't LISTEN here (e.g. a transaction-mode pooler). Polling covers it.
      if (this.listenerState !== "closed") this.listenerState = "closed";
    }
  }

  private onNotification(channel: string, payload: string | undefined): void {
    if (channel === "cairnq_queued") {
      if (this.queuedWaiters.size === 0) this.wakePending = true;
      for (const w of [...this.queuedWaiters]) w();
    } else if (channel === "cairnq_done" && payload) {
      for (const w of [...(this.doneWaiters.get(payload) ?? [])]) w();
    }
  }

  private dropListener(): void {
    const client = this.listener;
    this.listener = null;
    if (client) void client.end().catch(() => {});
    // Release everyone promptly; their fallback poll takes over.
    for (const w of [...this.queuedWaiters]) w();
    for (const set of [...this.doneWaiters.values()]) for (const w of [...set]) w();
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
