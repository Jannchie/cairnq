import type * as PG from "pg";

import { ProtocolVersionMismatch } from "../errors.js";
import { loadMigrations, loadStatements } from "../sql.js";
import { COMMENT, type Fetch, NAMED, type Params, statementParams, TaskStore } from "./base.js";

const SUPPORTED_PROTOCOL_MAJOR = 1;

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
 * Translate the protocol's named-parameter SQL (`:name`) into Postgres positional
 * placeholders (`$1`), collapsing each DISTINCT name to ONE slot — statements reuse
 * a name across CASE branches / IS NULL guards (e.g. list.sql). Which names count
 * as parameters is `statementParams`' decision, shared with the SQLite binding path
 * so the two can't disagree about, say, a `::type` cast. Names the statement does
 * not use are simply not bound, so callers may pass a superset. Exported for unit
 * testing.
 */
export function toPositional(
  sql: string,
  params: Params,
): { text: string; values: unknown[] } {
  const order = statementParams(sql);
  const slot = new Map(order.map((name, i) => [name, i + 1])); // 1-based $n
  const text = sql.replace(COMMENT, "").replace(NAMED, (_m, name: string) => `$${slot.get(name)}`);
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
        const version = await this.readProtocolVersion(client);
        if (version !== SUPPORTED_PROTOCOL_MAJOR) {
          throw new ProtocolVersionMismatch(
            `storage protocol_version=${version}, SDK supports ${SUPPORTED_PROTOCOL_MAJOR}`,
          );
        }
      } finally {
        client.release();
      }
    } catch (e) {
      await pool.end(); // never leak a pool when connect fails
      throw e;
    }
    this.pool = pool; // publish only a fully-migrated, version-checked pool
  }

  private async applyMigrations(client: PG.PoolClient): Promise<void> {
    await client.query(
      "create table if not exists cairnq_migrations " +
        "(name text primary key, applied_at_ms bigint not null)",
    );
    const applied = new Set(
      (await client.query("select name from cairnq_migrations")).rows.map(
        (r: { name: string }) => r.name,
      ),
    );
    for (const { name, sql } of loadMigrations("postgres")) {
      if (applied.has(name)) continue;
      try {
        await client.query("begin");
        await client.query(sql); // multi-statement DDL (simple-query, no params)
        await client.query(
          "insert into cairnq_migrations (name, applied_at_ms) values " +
            "($1, (extract(epoch from now()) * 1000)::bigint) on conflict (name) do nothing",
          [name],
        );
        await client.query("commit");
      } catch (e) {
        await client.query("rollback");
        throw e;
      }
    }
  }

  private async readProtocolVersion(client: PG.PoolClient): Promise<number> {
    const res = await client.query(
      "select value from cairnq_meta where key = 'protocol_version'",
    );
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
      await client.query("rollback");
      throw e;
    } finally {
      client.release();
    }
  }
}
