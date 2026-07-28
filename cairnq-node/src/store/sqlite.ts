import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

import Database from "better-sqlite3";

import { nowMs } from "../ids.js";
import { ProtocolVersionMismatch } from "../errors.js";
import { loadMigrations, loadStatements } from "../sql.js";
import { type Fetch, type Params, statementParams, TaskStore } from "./base.js";

type DB = Database.Database;
type Stmt = Database.Statement;

const SUPPORTED_PROTOCOL_MAJOR = 1;

/**
 * SQLiteStore — the SQLite dialect of the shared cairnq-protocol SQL.
 *
 * Everything protocol-shaped lives in TaskStore; this file is only what SQLite
 * does differently: better-sqlite3's synchronous driver, BEGIN IMMEDIATE
 * transactions, a read-only probe in front of the write lock, and time supplied
 * by the SDK (`:now_ms`) rather than by the database.
 *
 * The driver being synchronous suits SQLite's single writer: claim is one short
 * transaction, the handler runs outside any transaction, and
 * progress/heartbeat/succeed/fail are each their own short write. Cross-process
 * contention is absorbed by busy_timeout.
 */
export class SQLiteStore extends TaskStore {
  private db: DB | null = null;
  private stmts: Record<string, Stmt> = {};
  private readonly statements: Record<string, string>;
  // Serializes operations within this process. The driver is synchronous, but a
  // transaction here spans several awaits, and without this a second operation
  // would resume in the gap and land its statements inside the open transaction.
  private mutex: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly path: string,
    private readonly opts: { busyTimeoutMs?: number } = {},
  ) {
    super();
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

  // ------------------------------------------------------------ dialect seam
  /**
   * Adapt the dialect-neutral parameters to what this statement binds.
   *
   * SQLite statements carry no DB clock, so every absolute `*_ms` is derived here
   * from one `now`, and booleans cross as 0/1. The result is narrowed to the
   * names the SQL actually uses, which is what makes it safe for a caller to pass
   * one superset of parameters for both dialects.
   *
   * Each derivation writes a name Postgres does not use (`lease_until_ms` from
   * `lease_ms`, and so on), so a statement binds one or the other, never both —
   * which is why the derived values can be computed unconditionally and left for
   * the narrowing step to discard.
   */
  private bind(sql: string, params: Params): Params {
    const now = nowMs();
    const bound: Params = {};
    for (const name of statementParams(sql)) {
      switch (name) {
        case "now_ms":
          bound[name] = now;
          break;
        case "lease_until_ms":
          bound[name] = now + (params.lease_ms as number);
          break;
        case "run_at_ms":
          bound[name] = now + (params.delay_ms as number);
          break;
        case "before_ms":
          bound[name] = now - (params.older_than_ms as number);
          break;
        case "queues":
          bound[name] = JSON.stringify(params.queues);
          break;
        case "retryable":
        case "reset_attempt":
          bound[name] = params[name] ? 1 : 0;
          break;
        default:
          bound[name] = params[name];
      }
    }
    return bound;
  }

  private runNow(name: string, params: Params): any[] {
    const stmt = this.stmts[name];
    const bound = this.bind(this.statements[name], params);
    // Nearly every protocol statement ends in RETURNING; upsert_key does not, and
    // better-sqlite3 refuses .all() on a statement that yields no rows.
    if (!stmt.reader) {
      stmt.run(bound);
      return [];
    }
    return stmt.all(bound) as any[];
  }

  /** Serialize an operation against every other operation on this store. */
  private withLock<T>(fn: () => T | Promise<T>): Promise<T> {
    const run = this.mutex.then(fn, fn) as Promise<T>;
    this.mutex = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  protected async fetch(name: string, params: Params): Promise<any[]> {
    this.ensure();
    return this.withLock(() => this.runNow(name, params));
  }

  protected async tx<T>(fn: (fetch: Fetch) => Promise<T>): Promise<T> {
    const db = this.ensure();
    // BEGIN IMMEDIATE by hand rather than db.transaction(): the callback is async
    // (the seam is shared with Postgres), and better-sqlite3's wrapper only takes
    // a synchronous one. The lock above makes the manual version safe.
    return this.withLock(async () => {
      db.exec("BEGIN IMMEDIATE");
      try {
        const out = await fn(async (name, params) => this.runNow(name, params));
        db.exec("COMMIT");
        return out;
      } catch (err) {
        try {
          db.exec("ROLLBACK");
        } catch {
          // Already rolled back by SQLite (e.g. a constraint abort).
        }
        throw err;
      }
    });
  }

  protected async hasClaimableWork(params: Params): Promise<boolean> {
    // Read-only probe first: an idle worker never takes SQLite's single write
    // lock, so idle workers don't serialize against each other.
    const rows = await this.fetch("claimable_probe", params);
    return Boolean(rows[0]?.has_work);
  }
}
