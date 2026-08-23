import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";

import { createRequire } from "node:module";

import type Database from "better-sqlite3";

import { nowMs } from "../ids.js";
import { loadMigrations, loadStatements } from "../sql.js";
import { StoreClosed } from "../errors.js";
import {
  checkProtocolVersion,
  COMMENT,
  type Fetch,
  type Params,
  specialize,
  statementParams,
  TaskStore,
} from "./base.js";

// `better-sqlite3` is an optional dependency, matching `pg` on the Postgres side:
// a Postgres-only deployment should not have to build a native module it never
// loads, and importing this file must not pull one in. Required (not imported)
// because ensure() is synchronous — the open path applies migrations and cannot
// await — and createRequire gives a synchronous load from ESM.
const require = createRequire(import.meta.url);
let sqliteModule: typeof Database | null = null;
function loadSqlite(): typeof Database {
  if (sqliteModule) return sqliteModule;
  try {
    sqliteModule = require("better-sqlite3") as typeof Database;
  } catch {
    throw new Error(
      "SQLiteStore requires the 'better-sqlite3' package — install it (e.g. `npm i better-sqlite3`)",
    );
  }
  return sqliteModule;
}

type DB = Database.Database;
type Stmt = Database.Statement;

const WAL_RETRY_DELAY_MS = 50;
const WAL_RETRY_BUDGET_MS = 5_000;

const BUSY_RETRY_BASE_MS = 1;
const BUSY_RETRY_MAX_DELAY_MS = 50;

/**
 * How often a live connection revisits its planner statistics.
 *
 * Bounds how long the planner can work from a stale table shape; a minute is
 * arbitrary but small next to the days a worker holds its connection. It does not
 * set how often an ANALYZE actually runs — SQLite decides that itself, and only
 * once the table has diverged from its statistics by 10x, so a shorter interval
 * costs more no-ops (a few microseconds each) rather than more analyzing.
 */
const STATS_REFRESH_INTERVAL_MS = 60_000;

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
 * Whether this statement writes, and so belongs in a group commit.
 *
 * Read from the SQL rather than from a list of statement names, which would be a
 * second place to remember when the protocol gains a statement. Every protocol
 * statement is a single top-level `select`, `insert`, `update` or `delete`.
 *
 * Reads must stay out of the batch: `claimable_probe` exists precisely so an idle
 * worker never takes SQLite's write lock, and a BEGIN IMMEDIATE around it would
 * hand that back.
 */
function isWriteStatement(sql: string): boolean {
  return !/^\s*select/i.test(sql.replace(COMMENT, ""));
}

/**
 * One write waiting for its turn on the shared connection.
 *
 * The rows go back to the caller that asked for them, so a batch resolves each
 * member with its own result rather than a merged one.
 */
interface Pending {
  name: string;
  params: Params;
  resolve(rows: any[]): void;
  reject(err: unknown): void;
}

/**
 * Whether cairnq_tasks has been analyzed at all.
 *
 * Two steps because sqlite_stat1 does not exist until something runs ANALYZE, and
 * querying a missing table is an error rather than an empty result.
 */
function hasStatistics(db: DB): boolean {
  const table = db
    .prepare("select 1 from sqlite_master where type = 'table' and name = 'sqlite_stat1'")
    .get();
  if (!table) return false;
  return Boolean(
    db.prepare("select 1 from sqlite_stat1 where tbl = 'cairnq_tasks'").get(),
  );
}

/**
 * Bring cairnq_tasks' statistics up to date, cheaply enough to call on a timer.
 *
 * Without them the planner misreads `status = 'running'` as a large fraction of the
 * table and passes over the partial cairnq_tasks_lease_idx that lease recovery is
 * indexed for.
 *
 * The explicit bootstrap is not redundant with `PRAGMA optimize`. Before SQLite
 * 3.46 the pragma skips a table that has no sqlite_stat1 entry entirely — no mask
 * changes that, verified on 3.45.1 — so on those builds it can never produce the
 * *first* statistics, and the index stays unused for the life of the database.
 * Distro Pythons link exactly those builds (Ubuntu 24.04 ships 3.45.1), while
 * better-sqlite3 bundles its own newer one, so this is also what keeps the two SDKs
 * behaving alike rather than by luck of packaging.
 *
 * Once an entry exists, every version's pragma applies its own growth heuristic,
 * which is the part worth deferring to: it is a few microseconds when there is
 * nothing to do, where a bare ANALYZE would rescan the table every time.
 */
function refreshStatistics(db: DB): void {
  if (hasStatistics(db)) db.pragma("optimize");
  // Scoped to the one table whose shape the planner gets wrong; the key and meta
  // tables are read by primary key, where statistics change nothing. A database
  // this one shares with the caller's own tables is left alone.
  else db.exec("ANALYZE cairnq_tasks");
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
  /**
   * Prepared statements, keyed by the SQL they were prepared from rather than by
   * statement name: `specialize` gives a statement one text per set of optional
   * filters a caller supplies, and each of those is its own prepared plan — which
   * is the entire point, since the plan is what the specialization changes.
   * Bounded by the statement set times the filter combinations actually used.
   */
  private stmts = new Map<string, Stmt>();
  private readonly statements: Record<string, string>;
  /** This store's entry in `fileLocks` — see there for why it is per-database. */
  private readonly lockKey: string;
  /** How long a single operation may keep retrying a lost write lock. */
  private readonly busyBudgetMs: number;
  /** When this connection may next revisit its planner statistics. */
  private nextStatsRefreshAt = 0;
  /** Which statements are writes — see isWriteStatement. */
  private readonly writes: Record<string, boolean>;
  /** Writes waiting to be group-committed — see flush(). */
  private pending: Pending[] = [];
  /**
   * The flusher draining `pending`, or null when none is running — both the
   * "is one already going" guard and the handle `close()` waits on, which are
   * the same question. The Python twin keeps two fields only because its
   * _do_close hands the flusher off before awaiting it.
   */
  private flusher: Promise<void> | null = null;
  /** Set for the duration of close(), so the drain it waits on is finite. */
  private closing = false;
  /** The close in progress, so a second caller waits for it rather than
   * returning while the connection is still open. */
  private closed: Promise<void> | null = null;
  /** Whether a connection was ever opened — see ensure() on in-memory reopen. */
  private everOpened = false;

  constructor(
    private readonly path: string,
    opts: { busyTimeoutMs?: number } = {},
  ) {
    super();
    this.busyBudgetMs = opts.busyTimeoutMs ?? 5000;
    this.statements = loadStatements("sqlite");
    this.writes = Object.fromEntries(
      Object.entries(this.statements).map(([name, sql]) => [name, isWriteStatement(sql)]),
    );
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

  /**
   * Close the connection, once everything already accepted has landed.
   *
   * Two things can be in flight, and cutting off either loses a write that a
   * caller is still awaiting: a group commit holding a batch (`flush`), and a
   * transaction with a BEGIN IMMEDIATE open on this same connection (a keyed
   * submit, a claim). The first is awaited directly. The second is waited out by
   * queuing an empty operation behind it on the file lock — the lock is what
   * serializes them in the first place, so anything already queued there runs
   * before this does.
   *
   * Neither wait can be stretched indefinitely: `closing` turns away everything
   * that arrives from now on, so the queue this drains is the one that existed
   * when close() was called. Without that barrier a write landing between the
   * flusher finishing and the connection closing would start a fresh flusher
   * against a `db` already null — the TypeError that used to surface here.
   *
   * Closing does not retire the store. Connecting is lazy, so a store used again
   * afterwards reopens; `closing` is cleared for exactly that reason. The one
   * case where reopening cannot mean what the caller wants is an in-memory
   * database, whose contents live in the connection — see ensure().
   */
  async close(): Promise<void> {
    return (this.closed ??= this.doClose().finally(() => {
      this.closed = null;
    }));
  }

  private async doClose(): Promise<void> {
    this.closing = true;
    try {
      // Errors belong to the writes that carry them, and the flusher has already
      // delivered each one to its own waiter; close() only needs it finished.
      await this.flusher?.catch(() => {});
      // Runs behind every operation currently on the lock, including a
      // transaction mid-await. Reads are queued there too, so this also waits
      // out a statement that would otherwise land on a closed connection.
      await this.enqueue(() => {}).catch(() => {});
      if (this.db) {
        this.db.close();
        this.db = null;
        this.stmts.clear();
      }
    } finally {
      this.closing = false;
    }
  }

  private ensure(): DB {
    // A store mid-close is about to drop the connection this would hand back.
    // Refusing keeps close()'s drain finite, and gives the caller a typed error
    // instead of whatever the driver says about a connection pulled out from
    // under it.
    if (this.closing) throw new StoreClosed();
    if (this.db) return this.db;
    const memory = isMemory(this.path);
    // Reopening is how a store used again after close() carries on, and for a
    // file that is exactly right — the tasks are in the file. An in-memory
    // database lives in the connection, so the same path would hand back an
    // EMPTY store: every task the caller submitted gone, no error, and a `get`
    // that answers null as if the id had never existed. Say so instead.
    if (memory && this.everOpened) {
      throw new StoreClosed(
        `in-memory database ${JSON.stringify(this.path)} was closed — its contents ` +
          `went with the connection, so reopening would silently start from empty. ` +
          `Use a file path if the store needs to outlive a close.`,
      );
    }
    if (!memory) mkdirSync(dirname(this.path), { recursive: true });
    const db = new (loadSqlite())(this.path);
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
    // Give the query planner statistics (see refreshStatistics), repeated on a timer
    // from here on (see maybeRefreshStatistics).
    try {
      refreshStatistics(db);
    } catch (err) {
      // Statistics are an optimization, never correctness, so losing them to a
      // concurrent writer must not fail the open — the next one gets another
      // chance. Anything else is a real fault and belongs to the caller.
      if (!isBusy(err)) throw err;
    }
    this.nextStatsRefreshAt = Date.now() + STATS_REFRESH_INTERVAL_MS;
    // Warm the unfiltered texts, which every statement has and most callers use.
    // The specialized ones prepare on first use; see `stmts`.
    for (const sql of Object.values(this.statements)) {
      this.stmts.set(sql, db.prepare(sql));
    }
    this.db = db;
    this.everOpened = true;
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
        case "ids":
          // json_each needs a JSON array. Postgres binds the array itself as
          // text[], so only this dialect encodes.
          bound[name] = JSON.stringify(params[name]);
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
    const sql = specialize(this.statements[name], params);
    let stmt = this.stmts.get(sql);
    if (!stmt) {
      stmt = this.db!.prepare(sql);
      this.stmts.set(sql, stmt);
    }
    const bound = this.bind(sql, params);
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

  /**
   * Revisit this connection's planner statistics, at most once per
   * STATS_REFRESH_INTERVAL_MS.
   *
   * A connection lives for days, and the statements were prepared against whatever
   * the table looked like when it opened — a worker started against an empty
   * database plans as if it were still empty however large the backlog grows. The
   * prepared statements do pick the refreshed plans up: ANALYZE bumps the schema
   * cookie, so SQLite silently re-prepares them on next use. That is what makes
   * this worth doing rather than a restart-only concern.
   *
   * Queued rather than run under `withLock`: statistics are best-effort, so losing
   * the write lock to another process should cost nothing — skip and let the next
   * interval try, instead of spending an operation's whole retry budget on them.
   */
  private async maybeRefreshStatistics(db: DB): Promise<void> {
    const now = Date.now();
    if (now < this.nextStatsRefreshAt) return;
    // Claim the slot before running, not after: otherwise a burst of concurrent
    // operations all see it due and queue an ANALYZE apiece.
    this.nextStatsRefreshAt = now + STATS_REFRESH_INTERVAL_MS;
    try {
      await this.enqueue(() => refreshStatistics(db));
    } catch (err) {
      if (!isBusy(err)) throw err;
    }
  }

  /**
   * Group commit: one transaction for every write already waiting on the lock.
   *
   * A write costs microseconds to execute and a transaction costs a WAL commit, so
   * N concurrent writes spend nearly all their time on N commits they could have
   * shared. Measured at 200 finalizes: 80µs each one-transaction-apiece against
   * 10µs each in one transaction (`bench/sweep` sweep B).
   *
   * Nothing waits to form a batch — a flusher takes whatever arrived while the
   * previous one held the lock, so this trades no latency for the throughput. What
   * it does trade is atomicity: two callers' writes now land together or not at
   * all. Under at-least-once that is not observable (a lost batch is a
   * redelivery), and it is why every member is resolved only after COMMIT.
   */
  private flush(db: DB): void {
    // One writer waiting is the uncontended case, and it stays exactly as cheap as
    // before: wrapping a single statement in BEGIN/COMMIT would add two statements
    // to every write on an idle store.
    if (this.pending.length === 1) {
      const only = this.pending[0];
      let rows: any[];
      try {
        rows = this.runNow(only.name, only.params);
      } catch (err) {
        // Leave it pending on a lost write lock: withLock re-runs this flusher.
        if (isBusy(err)) throw err;
        this.pending.shift();
        only.reject(err);
        return;
      }
      this.pending.shift();
      only.resolve(rows);
      return;
    }

    // BEGIN before consuming, so a lost write lock leaves the batch where the
    // retry will find it — with anything that arrived meanwhile.
    db.exec("BEGIN IMMEDIATE");
    const batch = this.pending;
    this.pending = [];
    const out: { rows?: any[]; err?: unknown }[] = [];
    try {
      for (const w of batch) {
        try {
          out.push({ rows: this.runNow(w.name, w.params) });
        } catch (err) {
          // A statement error aborts that statement, not the transaction, so the
          // rest of the batch is still good and this one waiter carries the error.
          // If SQLite tore the transaction down instead, nothing in it survived
          // and every member has to hear about it.
          if (!db.inTransaction) throw err;
          out.push({ err });
        }
      }
      db.exec("COMMIT");
    } catch (err) {
      if (db.inTransaction) {
        try {
          db.exec("ROLLBACK");
        } catch {
          // Raced with SQLite's own rollback; the transaction is gone either way.
        }
      }
      if (isBusy(err)) {
        // Back to the head of the queue, ahead of later arrivals, so the retry
        // preserves the order the writes were issued in.
        this.pending = batch.concat(this.pending);
        throw err;
      }
      for (const w of batch) w.reject(err);
      return;
    }
    // Only now: before COMMIT a rollback could still take the write back, and a
    // caller holding its row would have observed a write that never happened.
    for (let i = 0; i < batch.length; i++) {
      // Presence, not truthiness — a thrown value is not guaranteed to be one.
      if ("err" in out[i]) batch[i].reject(out[i].err);
      else batch[i].resolve(out[i].rows!);
    }
  }

  /**
   * Make sure some flusher is draining `pending`, without ever running two.
   *
   * The flusher loops instead of re-arming itself per batch. A caller that awaits
   * its writes one at a time resumes and issues the next one *before* the flusher
   * gets its turn back, so re-arming would cost that write an extra trip through
   * the lock queue — measured as ~2x on sequential writes, which is most of them.
   * Looping picks it up in the same session for free.
   *
   * The exit is safe because the last `pending` check and clearing the flag happen
   * in one synchronous step: a write that arrives before it keeps the loop going,
   * and one that arrives after sees the flag down and starts a new flusher.
   */
  private scheduleFlush(db: DB): void {
    if (this.flusher) return;
    // Assigned before the `.finally` can possibly run — a finally callback is a
    // microtask at the earliest — so the field is never cleared before it is
    // set, however little `drain` does before returning.
    this.flusher = this.drain(db).finally(() => {
      this.flusher = null;
    });
  }

  /** The flusher's body; see scheduleFlush for why it is a separate method. */
  private async drain(db: DB): Promise<void> {
    while (this.pending.length) {
      try {
        await this.withLock(() => this.flush(db));
      } catch (err) {
        // flush only throws on a lost write lock, and only after putting its
        // batch back — so reaching here means withLock spent the whole budget
        // and those writes are still queued with nobody else coming for them.
        // Anything that arrived behind them is failed with the same error
        // rather than left hanging: this store cannot write at all right now,
        // which is what a lone write would have been told too.
        const stranded = this.pending;
        this.pending = [];
        for (const w of stranded) w.reject(err);
      }
    }
  }

  protected async fetch(name: string, params: Params): Promise<any[]> {
    const db = this.ensure();
    await this.maybeRefreshStatistics(db);
    // Reads keep their own turn on the lock — see isWriteStatement.
    if (!this.writes[name]) return this.withLock(() => this.runNow(name, params));
    return new Promise<any[]>((resolve, reject) => {
      this.pending.push({ name, params, resolve, reject });
      this.scheduleFlush(db);
    });
  }

  protected async tx<T>(fn: (fetch: Fetch) => Promise<T>): Promise<T> {
    const db = this.ensure();
    await this.maybeRefreshStatistics(db);
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

}
