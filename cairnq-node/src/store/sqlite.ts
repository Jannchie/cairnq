import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";

import Database from "better-sqlite3";

import { nowMs } from "../ids.js";
import { ProtocolVersionMismatch } from "../errors.js";
import { loadMigrations, loadStatements } from "../sql.js";
import { type Fetch, type Params, statementParams, TaskStore } from "./base.js";

type DB = Database.Database;
type Stmt = Database.Statement;

const SUPPORTED_PROTOCOL_MAJOR = 1;

const WAL_RETRY_DELAY_MS = 50;
const WAL_RETRY_BUDGET_MS = 5_000;

/** Sleep without yielding — the whole open path is synchronous already. */
function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/** Whether this path names an in-memory database rather than a file. */
function isMemory(path: string): boolean {
  return path === ":memory:" || path.includes("mode=memory");
}

/**
 * Serializes every SQLiteStore on one database file, process-wide.
 *
 * better-sqlite3 is synchronous, and a transaction holds SQLite's write lock
 * across `await`s (the callback seam is shared with Postgres, so it is async). A
 * second connection in this process then blocks the only thread waiting for that
 * lock, and the holder can never reach COMMIT — reaching it needs the thread the
 * waiter is sitting on. busy_timeout cannot break that inversion, being one
 * thread; the wait just burns the timeout and throws "database is locked". So the
 * two must not overlap at all.
 *
 * Keyed by database, not by store: what the lock protects is the file. Across
 * processes there is no inversion (the holder keeps its own thread) and
 * busy_timeout still applies. An in-memory database is private to one connection
 * and gets a key of its own.
 */
const fileLocks = new Map<string, Promise<unknown>>();
let memoryDbSeq = 0;

/**
 * Put the database in WAL mode, waiting out a concurrent cold start.
 *
 * journal_mode is a persistent property of the file, so only the first connection
 * to a new database actually switches it — and that switch needs an exclusive
 * lock. `busy_timeout` does not cover it: SQLite returns SQLITE_BUSY for a
 * journal_mode change rather than invoking the busy handler, so several processes
 * opening the same new database at once would otherwise get an instant "database
 * is locked". Retry briefly instead; the window is only as long as one other
 * opener's switch.
 *
 * Callers must skip in-memory databases: those report journal_mode = "memory" and
 * can never be WAL, so waiting for one is waiting for something that will not
 * happen.
 */
function enableWal(db: DB): void {
  const deadline = Date.now() + WAL_RETRY_BUDGET_MS;
  for (;;) {
    try {
      const rows = db.pragma("journal_mode = WAL") as { journal_mode?: string }[];
      if (rows[0]?.journal_mode?.toLowerCase() === "wal") return;
    } catch (err) {
      const message = String((err as Error).message ?? err);
      if (!/locked|busy/i.test(message)) throw err;
    }
    if (Date.now() >= deadline) {
      throw new Error(
        "could not switch the database to WAL mode: it stayed locked by another connection",
      );
    }
    sleepSync(WAL_RETRY_DELAY_MS);
  }
}

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
  /** This store's entry in `fileLocks` — see there for why it is per-database. */
  private readonly lockKey: string;

  constructor(
    private readonly path: string,
    private readonly opts: { busyTimeoutMs?: number } = {},
  ) {
    super();
    this.statements = loadStatements("sqlite");
    // Two `:memory:` handles share a path but not a database, so they must not
    // share a lock either.
    this.lockKey = isMemory(path) ? `memory#${memoryDbSeq++}` : resolve(path);
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
    const memory = isMemory(this.path);
    if (!memory) mkdirSync(dirname(this.path), { recursive: true });
    const db = new Database(this.path);
    // busy_timeout first, so every later statement waits out contention instead
    // of failing instantly.
    db.pragma(`busy_timeout = ${this.opts.busyTimeoutMs ?? 5000}`);
    // WAL exists so several processes can share one file. An in-memory database
    // is private to this connection, so there is nothing to share or wait for.
    if (!memory) enableWal(db);
    db.pragma("foreign_keys = ON");
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
    const isApplied = db.prepare("select 1 from cairnq_migrations where name = ?");
    const insert = db.prepare(
      "insert into cairnq_migrations (name, applied_at_ms) values (?, ?)",
    );
    for (const { name, sql } of loadMigrations("sqlite")) {
      // Check and apply under one write lock. Two processes cold-starting on a
      // shared database would otherwise both see a migration as unapplied and
      // both run it — harmless for the idempotent ones, not for a future ALTER.
      // `immediate` takes the write lock up front; the loser sees it applied.
      db.transaction(() => {
        if (isApplied.get(name)) return;
        db.exec(sql);
        insert.run(name, nowMs());
      }).immediate();
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

  /** Serialize an operation against every other operation on this database. */
  private withLock<T>(fn: () => T | Promise<T>): Promise<T> {
    const previous = fileLocks.get(this.lockKey) ?? Promise.resolve();
    const run = previous.then(fn, fn) as Promise<T>;
    fileLocks.set(
      this.lockKey,
      run.then(
        () => undefined,
        () => undefined,
      ),
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
