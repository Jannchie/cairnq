import type * as PG from "pg";

import { ListenUnavailable, type PgExecutor, type PgSession, type Row } from "./pg-executor.js";

// `pg` is an optional dependency: the SDK is SQLite-first, so it's loaded lazily
// the first time a pool-backed executor is built. Absent -> a clear install hint.
// An application that brings its own executor never reaches this.
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
  // Deliberately NOT setting a global int8 type parser here. It would be the
  // shortest fix for Postgres sending bigint as text, and it is what this file
  // used to do — but pg's type parsers are process-global, so a library that
  // installs one silently changes how the APPLICATION's own bigint columns come
  // back. rowToTask normalizes instead, which costs nothing and leaves the
  // caller's driver as they configured it.
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

/** The PgSession face of one pg client or pool. */
function session(q: Pick<PG.PoolClient, "query">): PgSession {
  return {
    async query(text: string, values: readonly unknown[]): Promise<Row[]> {
      return (await q.query(text, values as unknown[])).rows;
    },
    async exec(sql: string): Promise<void> {
      // No values: pg sends this over the simple query protocol, which is what
      // makes a multi-statement migration script legal here and not in query().
      await q.query(sql);
    },
  };
}

/**
 * A Postgres identifier that is safe to interpolate — cairnq quotes the schema
 * name, and a name that could close that quote could rewrite the statement.
 * Deliberately narrower than what Postgres accepts: a schema cairnq is asked to
 * live in is a deployment decision, not a place to be clever.
 */
const PLAIN_IDENT = /^[A-Za-z_][A-Za-z0-9_$]*$/;

/**
 * The built-in executor: a `pg.Pool` over a libpq DSN. What `CairnQ.postgres(dsn)`
 * uses, and the reference for what an adapter over another driver must do.
 *
 * Creating it does not connect — `pg.Pool` is lazy, and the store's first
 * statement (the migration ledger) is what proves the database is reachable.
 *
 * `schema` puts cairnq's tables in a schema of their own rather than in whatever
 * the connection's search_path leads with. The protocol's SQL names no schema, so
 * this is a per-connection `search_path` and not one statement changes.
 */
export async function createPoolExecutor(
  dsn: string,
  opts: { max?: number; schema?: string } = {},
): Promise<PgExecutor> {
  const pg = await loadPg();
  const schema = opts.schema;
  if (schema !== undefined && !PLAIN_IDENT.test(schema)) {
    throw new Error(
      `schema must be a plain identifier (letters, digits, _ and $, not starting ` +
        `with a digit), got ${JSON.stringify(schema)}`,
    );
  }
  const pool = new pg.Pool({ connectionString: dsn, max: opts.max });
  if (schema) {
    // Queued on the connection before it is handed out, so every statement this
    // pool ever runs — migrations included — resolves in the right schema. Pooled
    // connections come and go, which is why this is per-connection rather than a
    // one-off at startup.
    pool.on("connect", (client) => {
      void client.query(`set search_path to "${schema}"`);
    });
  }
  if (schema) {
    // Created once, here, rather than from the connect handler (which would ask
    // for CREATE privilege on every new connection) or from the migrations
    // (which name no schema, by design). Without it the first `create table`
    // fails with "no schema has been selected to create in", which says nothing
    // about the cause. This is the one thing that makes building the executor
    // connect; without `schema` it stays lazy.
    try {
      await pool.query(`create schema if not exists "${schema}"`);
    } catch (e) {
      await pool.end();
      throw e;
    }
  }
  const poolSession = session(pool);

  return {
    query: poolSession.query,
    exec: poolSession.exec,

    async tx<T>(fn: (s: PgSession) => Promise<T>): Promise<T> {
      const client = await pool.connect();
      try {
        await client.query("begin");
        const out = await fn(session(client));
        await client.query("commit");
        return out;
      } catch (e) {
        await rollbackQuietly(client);
        throw e;
      } finally {
        client.release();
      }
    },

    async listen(channels, onNotify, onClose): Promise<() => void> {
      // Built from the raw DSN rather than taken from the pool: a listener holds
      // its connection for its whole life, and a pooled one would be a slot the
      // store never gives back. If `opts` ever grows connection-level settings
      // (ssl, application_name), the listener must receive them too.
      const client = new pg.Client({ connectionString: dsn });
      await client.connect(); // failure here is transient: caller retries with backoff
      client.on("notification", (msg) => onNotify(msg.channel, msg.payload));
      // A dropped listener degrades to polling; the store reconnects on the next wake.
      client.on("error", () => onClose());
      try {
        await client.query(channels.map((c) => `listen ${c}`).join("; "));
      } catch (e) {
        void client.end().catch(() => {});
        // Connected, but LISTEN was refused (e.g. a transaction-mode pooler) —
        // deterministic, so tell the store not to retry.
        throw new ListenUnavailable(e instanceof Error ? e.message : undefined);
      }
      return () => void client.end().catch(() => {});
    },

    async close(): Promise<void> {
      await pool.end();
    },
  };
}
