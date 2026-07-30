import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";

import Database from "better-sqlite3";

import { nowMs } from "../ids.js";
import { loadMigrations, loadStatements } from "../sql.js";
import {
  checkProtocolVersion,
  type Fetch,
  type Params,
  statementParams,
  TaskStore,
} from "./base.js";

type DB = Database.Database;
type Stmt = Database.Statement;

const WAL_RETRY_DELAY_MS = 50;
const WAL_RETRY_BUDGET_MS = 5_000;

const BUSY_RETRY_BASE_MS = 1;
const BUSY_RETRY_MAX_DELAY_MS = 50;

/** Sleep without yielding — the whole open path is synchronous already. */
function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/** Sleep by yielding to the event loop — the point of the busy retry loop. */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Whether this error is SQLite refusing to wait for the write lock.
 *
 * Prefix match: the code carries detail suffixes (`SQLITE_BUSY_SNAPSHOT`). Only
 * SQLITE_BUSY qualifies — SQLITE_LOCKED is same-connection table contention,
 * which the per-file lock prevents and a retry could not resolve anyway.
 */
function isBusy(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const code = (err as { code?: unknown }).code;
  return typeof code === "string" && code.startsWith("SQLITE_BUSY");
}

/** Whether this path names an in-memory database rather than a file. */
function isMemory(path: string): boolean {
  return path === ":memory:" || path.includes("mode=memory");
}

/**
 * Serializes every SQLiteStore on one database file, process-wide.
 *
 * better-sqlite3 is synchronous, and a transaction holds SQLite's write lock
 * across `await`s (the callback seam is shared with Postgres, so it is async). Two
 * connections in this process would then contend for that lock the expensive way:
 * every loser spends SQLITE_BUSY retries and backoff on a holder it could simply
 * have queued behind.
 *
 * Keyed by database, not by store: what the lock protects is the file. Across
 * processes there is nothing to serialize from here — each holder has its own
 * thread, and `withLock`'s retry absorbs that contention. An in-memory database is
 * private to one connection and gets a key of its own.
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
      if (!isBusy(err)) throw err;
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
 * progress/heartbeat/succeed/fail are each their own short write.
 *
 * Cross-process contention is absorbed by retrying in JavaScript, not by
 * busy_timeout. The two cost the same wait but not the same blocking: a nonzero
 * busy_timeout waits *inside* the synchronous driver, so a caller that loses the
 * write lock stalls this process's event loop for up to the whole timeout — the
 * P99 of an HTTP server that submits tasks. Executing a statement takes
 * microseconds; waiting for a lock takes milliseconds to seconds, and only the
 * second part needs to happen off the thread. So busy_timeout goes to 0 (fail
 * immediately) and the wait becomes an awaited backoff, which the event loop runs
 * through. The budget is the same either way — `busyTimeoutMs`.
 *
 * The open path keeps a real busy_timeout: it is synchronous by nature (WAL
 * switch, migrations) and happens once, under the caller's `connect()`.
 */
export class SQLiteStore extends TaskStore {
  private db: DB | null = null;
  private stmts: Record<string, Stmt> = {};
  private readonly statements: Record<string, string>;
  /** This store's entry in `fileLocks` — see there for why it is per-database. */
  private readonly lockKey: string;
  /** How long a single operation may keep retrying a lost write lock. */
  private readonly busyBudgetMs: number;

  constructor(
    private readonly path: string,
    opts: { busyTimeoutMs?: number } = {},
  ) {
    super();
    this.busyBudgetMs = opts.busyTimeoutMs ?? 5000;
    this.statements = loadStatements("sqlite");
    // Only a bare ":memory:" is guaranteed private to its connection, so only
    // it gets a lock of its own. A "mode=memory" URI stays path-keyed: with
    // cache=shared it names ONE shared database, and on a build without URI
    // filenames it is a literal file — in both cases two stores on that string
    // must share a lock. Over-serializing a private URI-memory database is
    // harmless; skipping the lock on a shared one is the deadlock this map
    // exists to prevent.
    this.lockKey = path === ":memory:" ? `memory#${memoryDbSeq++}` : resolve(path);
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
    // Only the synchronous part of the open path gets a real busy_timeout: the WAL
    // switch and the migrations cannot await a retry. See the class comment.
    db.pragma(`busy_timeout = ${this.busyBudgetMs}`);
    // WAL exists so several processes can share one file. An in-memory database
    // is private to this connection, so there is nothing to share or wait for.
    if (!memory) enableWal(db);
    db.pragma("foreign_keys = ON");
    this.applyMigrations(db);
    // Everything past here either awaits its retry or is optional, so stop blocking.
    db.pragma("busy_timeout = 0");
    // Give the query planner statistics: without sqlite_stat1 it misreads
    // `status = 'running'` as a large fraction of the table and passes over the
    // partial cairnq_tasks_lease_idx that lease recovery is indexed for. PRAGMA
    // optimize decides for itself whether an ANALYZE is worth running — a no-op on
    // a fresh or little-changed database — and must precede the prepare loop below,
    // which is what bakes the resulting plans in.
    //
    // Open-time only: a worker holds its connection for days, so a database that
    // grows an order of magnitude mid-session plans against its startup shape until
    // it restarts.
    try {
      db.pragma("optimize");
    } catch (err) {
      // Statistics are an optimization, never correctness, so losing them to a
      // concurrent writer must not fail the open — the next one gets another
      // chance. Anything else is a real fault and belongs to the caller.
      if (!isBusy(err)) throw err;
    }
    for (const [name, sql] of Object.entries(this.statements)) {
      this.stmts[name] = db.prepare(sql);
    }
    this.db = db;
    checkProtocolVersion(this.readProtocolVersion());
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

  private readProtocolVersion(): number {
    const rows = this.runNow("protocol_version", {});
    return rows.length ? Number(rows[0].value) : 0;
  }

  async protocolVersion(): Promise<number> {
    this.ensure();
    // Under the store lock: this public read must not slip a statement into
    // another operation's open transaction on the shared connection.
    return this.withLock(() => this.readProtocolVersion());
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
        case "names":
          // json_each needs a JSON array; null stays null so the SQL's
          // `:names is null` arm means "no filter".
          bound[name] = params.names == null ? null : JSON.stringify(params.names);
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

  /** Queue an operation behind every other operation on this database. */
  private enqueue<T>(fn: () => T | Promise<T>): Promise<T> {
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

  /**
   * Serialize an operation against this database, waiting out a lost write lock on
   * a jittered backoff. Replaces busy_timeout's synchronous wait (see the class
   * comment); on exhausting the budget the original SQLITE_BUSY surfaces, which is
   * what a nonzero busy_timeout would have thrown too.
   *
   * Each attempt re-queues rather than backing off while holding its turn: the
   * contention left to retry is cross-process, and under WAL a *reader* never sees
   * SQLITE_BUSY at all — so sleeping in place would stall this process's reads
   * (including the worker's own poll) on a lock they were never waiting for.
   *
   * Retrying is safe because an attempt is one statement, or one transaction that
   * has already rolled back: nothing partially applied survives it. `fn` may
   * therefore run more than once and must not carry effects of its own — the
   * callers in TaskStore build their ids and payloads before opening one.
   */
  private async withLock<T>(fn: () => T | Promise<T>): Promise<T> {
    const deadline = Date.now() + this.busyBudgetMs;
    let delay = BUSY_RETRY_BASE_MS;
    for (;;) {
      try {
        return await this.enqueue(fn);
      } catch (err) {
        if (!isBusy(err) || Date.now() >= deadline) throw err;
        // Jitter so several losers don't wake together and collide again.
        await sleep(delay * (0.5 + Math.random()));
        delay = Math.min(delay * 2, BUSY_RETRY_MAX_DELAY_MS);
      }
    }
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
      // With busy_timeout at 0 this is where a lost write lock surfaces, and it
      // fails before the transaction exists — so the retry re-runs `fn` cleanly.
      db.exec("BEGIN IMMEDIATE");
      try {
        const out = await fn(async (name, params) => this.runNow(name, params));
        db.exec("COMMIT");
        return out;
      } catch (err) {
        // Nothing to roll back when BEGIN was what failed — the common case under
        // contention — or when SQLite already did it (a constraint abort).
        if (db.inTransaction) {
          try {
            db.exec("ROLLBACK");
          } catch {
            // Raced with SQLite's own rollback; the transaction is gone either way.
          }
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
