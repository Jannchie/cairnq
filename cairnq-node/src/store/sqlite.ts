import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

import Database from "better-sqlite3";

import { newId, nowMs } from "../ids.js";
import { AlreadyExists, errorEnvelope, LostLease, ProtocolVersionMismatch } from "../errors.js";
import { rowToTask, type Task } from "../models.js";
import { loadMigrations, loadStatements } from "../sql.js";
import type { ListInput, SubmitInput, TaskStore } from "./base.js";

type DB = Database.Database;
type Stmt = Database.Statement;

const SUPPORTED_PROTOCOL_MAJOR = 1;

const LEASE_EXPIRED_ERROR = errorEnvelope({
  type: "LeaseExpired",
  code: "lease_expired",
  message: "task lease expired and max attempts reached",
  retryable: false,
});
// Serialized once: it's an immutable constant bound on every claim that finds work.
const LEASE_EXPIRED_ERROR_JSON = JSON.stringify(LEASE_EXPIRED_ERROR);

/**
 * SQLiteStore — better-sqlite3 backend executing the shared cairnq-protocol SQL.
 *
 * The driver is synchronous, which suits SQLite's single writer: claim is one
 * short transaction, the handler runs outside any transaction, and
 * progress/heartbeat/succeed/fail are each their own short write. JS being
 * single-threaded means sync DB calls never interleave. Cross-process contention
 * (deployment mode B) is absorbed by busy_timeout.
 */
export class SQLiteStore implements TaskStore {
  private db: DB | null = null;
  private stmts: Record<string, Stmt> = {};
  private readonly statements: Record<string, string>;

  constructor(
    private readonly path: string,
    private readonly opts: { busyTimeoutMs?: number } = {},
  ) {
    this.statements = loadStatements("sqlite");
  }

  async connect(): Promise<void> {
    this.ensure();
  }

  async close(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = null;
      this.stmts = {};
    }
  }

  private ensure(): DB {
    if (this.db) return this.db;
    if (this.path !== ":memory:") mkdirSync(dirname(this.path), { recursive: true });
    const db = new Database(this.path);
    db.pragma("journal_mode = WAL");
    db.pragma("foreign_keys = ON");
    db.pragma(`busy_timeout = ${this.opts.busyTimeoutMs ?? 5000}`);
    this.applyMigrations(db);
    for (const [name, sql] of Object.entries(this.statements)) {
      this.stmts[name] = db.prepare(sql);
    }
    this.db = db;
    this.checkVersion();
    return db;
  }

  private applyMigrations(db: DB): void {
    db.exec(
      "create table if not exists cairnq_migrations " +
        "(name text primary key, applied_at_ms integer not null)",
    );
    const applied = new Set(
      (db.prepare("select name from cairnq_migrations").all() as { name: string }[]).map(
        (r) => r.name,
      ),
    );
    // `or ignore`: another process may apply the same migration concurrently on a
    // fresh shared db (mode B cold start). Migrations are idempotent.
    const insert = db.prepare(
      "insert or ignore into cairnq_migrations (name, applied_at_ms) values (?, ?)",
    );
    for (const { name, sql } of loadMigrations("sqlite")) {
      if (applied.has(name)) continue;
      db.transaction(() => {
        db.exec(sql);
        insert.run(name, nowMs());
      })();
    }
  }

  private checkVersion(): void {
    const version = this.readProtocolVersion();
    if (version !== SUPPORTED_PROTOCOL_MAJOR) {
      throw new ProtocolVersionMismatch(
        `storage protocol_version=${version}, SDK supports ${SUPPORTED_PROTOCOL_MAJOR}`,
      );
    }
  }

  private readProtocolVersion(): number {
    const row = this.db!
      .prepare("select value from cairnq_meta where key = 'protocol_version'")
      .get() as { value: string } | undefined;
    return row ? Number(row.value) : 0;
  }

  async protocolVersion(): Promise<number> {
    this.ensure();
    return this.readProtocolVersion();
  }

  private all(name: string, params: Record<string, unknown>): any[] {
    return this.stmts[name].all(params) as any[];
  }

  private run(name: string, params: Record<string, unknown>): void {
    this.stmts[name].run(params);
  }

  // An ownership-checked worker write (heartbeat/progress/succeed/complete/fail).
  // Each statement's WHERE pins worker_id + a live lease, so 0 rows back means the
  // lease was lost — every such write reports it the same way.
  private ownedWrite(name: string, taskId: string, params: Record<string, unknown>): Task {
    const rows = this.all(name, params);
    if (!rows.length) throw new LostLease(taskId);
    return rowToTask(rows[0]);
  }

  // ------------------------------------------------------------- client side
  async submit(input: SubmitInput): Promise<Task> {
    this.ensure();
    const now = nowMs();
    const id = newId("task");
    const ins = {
      id,
      name: input.name,
      queue: input.queue ?? "default",
      payload: JSON.stringify(input.payload ?? {}),
      metadata: JSON.stringify(input.metadata ?? {}),
      max_attempts: input.maxAttempts ?? 3,
      priority: input.priority ?? 0,
      run_at_ms: now + (input.runAtDelayMs ?? 0),
      parent_id: input.parentId ?? null,
      root_id: input.rootId ?? id,
      correlation_id: input.correlationId ?? null,
      now_ms: now,
    };
    const key = input.key ?? null;
    const conflict = input.conflict ?? "reuse";

    const txn = this.db!.transaction(() => {
      if (key === null) return this.all("insert_task", ins)[0];
      const existing = this.all("get_key", { key }) as { task_id: string }[];
      if (existing.length) {
        const exId = existing[0].task_id;
        if (conflict === "reuse") return this.all("get", { id: exId })[0];
        if (conflict === "reject") throw new AlreadyExists(key);
        if (conflict === "replace") {
          this.all("cancel", { id: exId, now_ms: now });
          const row = this.all("insert_task", ins)[0];
          this.run("upsert_key", { key, task_id: id, now_ms: now });
          return row;
        }
        throw new Error(`unknown conflict strategy: ${conflict}`);
      }
      const row = this.all("insert_task", ins)[0];
      this.run("upsert_key", { key, task_id: id, now_ms: now });
      return row;
    });
    return rowToTask(txn.immediate());
  }

  async get(taskId: string): Promise<Task | null> {
    this.ensure();
    const rows = this.all("get", { id: taskId });
    return rows.length ? rowToTask(rows[0]) : null;
  }

  async getByKey(key: string): Promise<Task | null> {
    this.ensure();
    const rows = this.all("get_by_key", { key });
    return rows.length ? rowToTask(rows[0]) : null;
  }

  async list(input: ListInput = {}): Promise<Task[]> {
    this.ensure();
    const rows = this.all("list", {
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
    this.ensure();
    const rows = this.all("cancel", { id: taskId, now_ms: nowMs() });
    return rows.length ? rowToTask(rows[0]) : null;
  }

  async cancelByKey(key: string): Promise<Task | null> {
    this.ensure();
    const txn = this.db!.transaction(() => {
      const existing = this.all("get_key", { key }) as { task_id: string }[];
      if (!existing.length) return null;
      const rows = this.all("cancel", { id: existing[0].task_id, now_ms: nowMs() });
      return rows.length ? rows[0] : null;
    });
    const row = txn.immediate();
    return row ? rowToTask(row) : null;
  }

  async retry(taskId: string, opts: { resetAttempt?: boolean } = {}): Promise<Task | null> {
    this.ensure();
    const rows = this.all("retry", {
      id: taskId,
      now_ms: nowMs(),
      reset_attempt: opts.resetAttempt ? 1 : 0,
    });
    return rows.length ? rowToTask(rows[0]) : null;
  }

  async retryByKey(key: string, opts: { resetAttempt?: boolean } = {}): Promise<Task | null> {
    this.ensure();
    const txn = this.db!.transaction(() => {
      const existing = this.all("get_key", { key }) as { task_id: string }[];
      if (!existing.length) return null;
      const rows = this.all("retry", {
        id: existing[0].task_id,
        now_ms: nowMs(),
        reset_attempt: opts.resetAttempt ? 1 : 0,
      });
      return rows.length ? rows[0] : null;
    });
    const row = txn.immediate();
    return row ? rowToTask(row) : null;
  }

  // ------------------------------------------------------------- worker side
  async claim(input: {
    queues: string[];
    workerId: string;
    leaseMs?: number;
    limit?: number;
  }): Promise<Task[]> {
    this.ensure();
    const now = nowMs();
    const queues = JSON.stringify(input.queues);
    const leaseMs = input.leaseMs ?? 30_000;
    const limit = input.limit ?? 1;
    // Read-only probe first: skip the write lock entirely when idle.
    const probe = this.all("claimable_probe", { queues, now_ms: now })[0] as { has_work: number };
    if (!probe || !probe.has_work) return [];
    const txn = this.db!.transaction(() => {
      this.all("recover_leases", {
        now_ms: now,
        lease_expired_error: LEASE_EXPIRED_ERROR_JSON,
      });
      return this.all("claim", {
        queues,
        now_ms: now,
        worker_id: input.workerId,
        lease_until_ms: now + leaseMs,
        limit,
      });
    });
    return (txn.immediate() as any[]).map(rowToTask);
  }

  async heartbeat(input: {
    taskId: string;
    workerId: string;
    leaseMs?: number;
  }): Promise<Task> {
    this.ensure();
    const now = nowMs();
    return this.ownedWrite("heartbeat", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      now_ms: now,
      lease_until_ms: now + (input.leaseMs ?? 30_000),
    });
  }

  async progress(input: {
    taskId: string;
    workerId: string;
    progress: number | null;
    message: string | null;
  }): Promise<Task> {
    this.ensure();
    return this.ownedWrite("progress", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      now_ms: nowMs(),
      progress: input.progress,
      message: input.message,
    });
  }

  async succeed(input: { taskId: string; workerId: string; result: unknown }): Promise<Task> {
    this.ensure();
    return this.ownedWrite("succeed", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      now_ms: nowMs(),
      result: input.result == null ? null : JSON.stringify(input.result),
      message: null,
    });
  }

  async complete(input: { taskId: string; workerId: string; result: unknown }): Promise<Task> {
    this.ensure();
    return this.ownedWrite("complete", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      now_ms: nowMs(),
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
    this.ensure();
    return this.ownedWrite("fail", input.taskId, {
      id: input.taskId,
      worker_id: input.workerId,
      now_ms: nowMs(),
      error: JSON.stringify(input.error ?? {}),
      retryable: input.retryable === false ? 0 : 1,
      delay_ms: input.delayMs ?? 0,
    });
  }
}
