import type * as PG from "pg";

import { newId } from "../ids.js";
import { AlreadyExists, errorEnvelope, LostLease, ProtocolVersionMismatch } from "../errors.js";
import { rowToTask, type Task } from "../models.js";
import { loadMigrations, loadStatements } from "../sql.js";
import type { ListInput, SubmitInput, TaskStore } from "./base.js";

const SUPPORTED_PROTOCOL_MAJOR = 1;

const LEASE_EXPIRED_ERROR_JSON = JSON.stringify(
  errorEnvelope({
    type: "LeaseExpired",
    code: "lease_expired",
    message: "task lease expired and max attempts reached",
    retryable: false,
  }),
);

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
 * a name across CASE branches / IS NULL guards (e.g. list.sql). SQL comments are
 * stripped first so a `:name` mentioned in a header comment (e.g. "now + :lease_ms")
 * never leaks into the parameter list. Exported for unit testing.
 */
export function toPositional(
  sql: string,
  params: Record<string, unknown>,
): { text: string; values: unknown[] } {
  const body = sql.replace(/--[^\n]*/g, "");
  const order: string[] = [];
  const slot = new Map<string, number>();
  const text = body.replace(/(?<!:):(\w+)/g, (_m, name: string) => {
    let i = slot.get(name);
    if (i === undefined) {
      order.push(name);
      i = order.length; // 1-based $n
      slot.set(name, i);
    }
    return `$${i}`;
  });
  return { text, values: order.map((n) => params[n]) };
}

/**
 * PostgresStore — asyncpg's TS counterpart: a `pg` Pool executing the shared
 * cairnq-protocol SQL (postgres dialect). Multi-host capable — unlike SQLite this
 * coordinates API and worker processes across machines through one database. Time
 * comes from the DB clock (now()), claim uses FOR UPDATE SKIP LOCKED, JSON columns
 * are jsonb (bound as JSON text, read back as objects by rowToTask's adaptive parse).
 */
export class PostgresStore implements TaskStore {
  private pool: PG.Pool | null = null;
  private connecting: Promise<void> | null = null;
  private readonly statements: Record<string, string>;

  constructor(
    private readonly dsn: string,
    private readonly opts: { max?: number } = {},
  ) {
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

  // ------------------------------------------------------------------ helpers
  private async run(name: string, params: Record<string, unknown>): Promise<any[]> {
    const { text, values } = toPositional(this.statements[name], params);
    return (await this.pool!.query(text, values)).rows;
  }

  private async runOn(
    client: PG.PoolClient,
    name: string,
    params: Record<string, unknown>,
  ): Promise<any[]> {
    const { text, values } = toPositional(this.statements[name], params);
    return (await client.query(text, values)).rows;
  }

  // BEGIN … COMMIT on a dedicated pool client, rolling back on any error.
  private async tx<T>(fn: (client: PG.PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool!.connect();
    try {
      await client.query("begin");
      const out = await fn(client);
      await client.query("commit");
      return out;
    } catch (e) {
      await client.query("rollback");
      throw e;
    } finally {
      client.release();
    }
  }

  // An ownership-checked worker write: each statement's WHERE pins worker_id + a
  // live lease, so 0 rows back means the lease was lost.
  private async ownedWrite(
    name: string,
    taskId: string,
    params: Record<string, unknown>,
  ): Promise<Task> {
    const rows = await this.run(name, params);
    if (!rows.length) throw new LostLease(taskId);
    return rowToTask(rows[0]);
  }

  // ------------------------------------------------------------- client side
  async submit(input: SubmitInput): Promise<Task> {
    await this.ensure();
    const id = newId("task");
    // No now_ms / run_at_ms: the DB clock supplies time, so submit passes a
    // relative :delay_ms and the SQL computes run_at = now + delay.
    const ins = {
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
    if (key === null) return rowToTask((await this.run("insert_task", ins))[0]);

    return this.tx(async (client) => {
      const existing = (await this.runOn(client, "get_key", { key })) as { task_id: string }[];
      if (existing.length) {
        const exId = existing[0].task_id;
        if (conflict === "reuse") return rowToTask((await this.runOn(client, "get", { id: exId }))[0]);
        if (conflict === "reject") throw new AlreadyExists(key);
        if (conflict === "replace") {
          await this.runOn(client, "cancel", { id: exId });
          const row = (await this.runOn(client, "insert_task", ins))[0];
          await this.runOn(client, "upsert_key", { key, task_id: id });
          return rowToTask(row);
        }
        throw new Error(`unknown conflict strategy: ${conflict}`);
      }
      const row = (await this.runOn(client, "insert_task", ins))[0];
      await this.runOn(client, "upsert_key", { key, task_id: id });
      return rowToTask(row);
    });
  }

  async get(taskId: string): Promise<Task | null> {
    await this.ensure();
    const rows = await this.run("get", { id: taskId });
    return rows.length ? rowToTask(rows[0]) : null;
  }

  async getByKey(key: string): Promise<Task | null> {
    await this.ensure();
    const rows = await this.run("get_by_key", { key });
    return rows.length ? rowToTask(rows[0]) : null;
  }

  async list(input: ListInput = {}): Promise<Task[]> {
    await this.ensure();
    const rows = await this.run("list", {
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
    await this.ensure();
    const rows = await this.run("cancel", { id: taskId });
    return rows.length ? rowToTask(rows[0]) : null;
  }

  async cancelByKey(key: string): Promise<Task | null> {
    await this.ensure();
    return this.tx(async (client) => {
      const existing = (await this.runOn(client, "get_key", { key })) as { task_id: string }[];
      if (!existing.length) return null;
      const rows = await this.runOn(client, "cancel", { id: existing[0].task_id });
      return rows.length ? rowToTask(rows[0]) : null;
    });
  }

  async retry(taskId: string, opts: { resetAttempt?: boolean } = {}): Promise<Task | null> {
    await this.ensure();
    const rows = await this.run("retry", { id: taskId, reset_attempt: opts.resetAttempt ?? false });
    return rows.length ? rowToTask(rows[0]) : null;
  }

  async retryByKey(key: string, opts: { resetAttempt?: boolean } = {}): Promise<Task | null> {
    await this.ensure();
    return this.tx(async (client) => {
      const existing = (await this.runOn(client, "get_key", { key })) as { task_id: string }[];
      if (!existing.length) return null;
      const rows = await this.runOn(client, "retry", {
        id: existing[0].task_id,
        reset_attempt: opts.resetAttempt ?? false,
      });
      return rows.length ? rowToTask(rows[0]) : null;
    });
  }

  // ------------------------------------------------------------- worker side
  async claim(input: {
    queues: string[];
    workerId: string;
    leaseMs?: number;
    limit?: number;
  }): Promise<Task[]> {
    await this.ensure();
    // No claimable_probe: PG readers don't block writers, so a plain transaction
    // (recover then claim) is cheap even when idle. FOR UPDATE SKIP LOCKED in
    // claim.sql gives true concurrent, non-contending dispatch.
    return this.tx(async (client) => {
      await this.runOn(client, "recover_leases", { lease_expired_error: LEASE_EXPIRED_ERROR_JSON });
      const rows = await this.runOn(client, "claim", {
        queues: input.queues,
        worker_id: input.workerId,
        lease_ms: input.leaseMs ?? 30_000,
        limit: input.limit ?? 1,
      });
      return rows.map(rowToTask);
    });
  }

  async heartbeat(input: { taskId: string; workerId: string; leaseMs?: number }): Promise<Task> {
    await this.ensure();
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
    await this.ensure();
    return this.ownedWrite("progress", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      progress: input.progress,
      message: input.message,
    });
  }

  async succeed(input: { taskId: string; workerId: string; result: unknown }): Promise<Task> {
    await this.ensure();
    return this.ownedWrite("succeed", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      result: input.result == null ? null : JSON.stringify(input.result),
      message: null,
    });
  }

  async complete(input: { taskId: string; workerId: string; result: unknown }): Promise<Task> {
    await this.ensure();
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
    await this.ensure();
    return this.ownedWrite("fail", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      error: JSON.stringify(input.error ?? {}),
      retryable: input.retryable !== false,
      delay_ms: input.delayMs ?? 0,
    });
  }
}
