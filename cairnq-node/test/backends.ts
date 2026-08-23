/**
 * Run one behavioural suite against every backend it claims to support.
 *
 * Until this existed, "does this test touch Postgres?" was an EMERGENT property
 * of whether the file happened to read `CAIRNQ_TEST_PG_DSN` — 2 of 28 files did,
 * so lease recovery, backpressure, retention, batch delivery and the keyed
 * transactions were only ever proven on SQLite. The two dialects differ in ways
 * those suites are exactly the right shape to catch: the clock (`:now_ms` from
 * the SDK vs `now()` from the database), the key lock (a no-op on SQLite, an
 * advisory lock on Postgres), the claim (`BEGIN IMMEDIATE` vs
 * `FOR UPDATE SKIP LOCKED`), and which optional filters reach an index.
 *
 * So the backend set becomes a DECLARATION. A suite that runs on one dialect
 * says which and why, and that claim is reviewable — where before, a suite that
 * silently covered one dialect looked exactly like a suite that covered both.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe } from "vitest";
import pg from "pg";

import { CairnQ, type ClientOptions, Worker, type WorkerOptions } from "../src/index.js";
import { freshDbPath } from "./helpers.js";

export type BackendName = "sqlite" | "postgres";

/** Everything a suite needs to reach the store under test. */
export interface Backend {
  readonly name: BackendName;
  /**
   * A connected client on this test's own empty database. Closed automatically
   * when the test ends, so suites need no try/finally of their own.
   */
  client(opts?: ClientOptions): Promise<CairnQ>;
  /** A worker on the same database. Not connected — run()/serve() does that. */
  worker(opts?: WorkerOptions & { queues?: string[] }): Worker;
}

/** The DSN CI's `postgres` service provides; absent locally unless you set it. */
const PG_DSN = process.env.CAIRNQ_TEST_PG_DSN;

/** A legal Postgres database name derived from a suite title. */
function dbNameFor(title: string): string {
  const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return `cairnq_t_${slug}`.slice(0, 60);
}

/**
 * A database per SUITE rather than per test file, and a truncate per test.
 *
 * Vitest runs files in parallel, so a shared database would have one file's
 * truncate deleting another file's rows mid-test. A database of its own also
 * keeps NOTIFY out of it: channel names are database-scoped (see PROTOCOL.md
 * "Push wakeups"), so suites sharing one database would wake each other's
 * workers and a watch test could not tell a real signal from a neighbour's.
 */
class PostgresBackend implements Backend {
  readonly name = "postgres" as const;
  private dsn = "";
  private admin: pg.Pool | null = null;
  private open: CairnQ[] = [];

  constructor(private readonly title: string) {}

  async createDatabase(): Promise<void> {
    const name = dbNameFor(this.title);
    const maintenance = new pg.Pool({ connectionString: PG_DSN });
    // Dropped first: a previous run that died before its teardown would
    // otherwise hand this one a database full of its rows.
    await maintenance.query(`drop database if exists ${name}`).catch(() => {});
    await maintenance.query(`create database ${name}`).catch(() => {});
    await maintenance.end();
    this.dsn = Object.assign(new URL(PG_DSN!), { pathname: `/${name}` }).toString();
    // Once, here: migrations are idempotent but not free, and every test in the
    // suite connects to the same database.
    const migrator = CairnQ.postgres(this.dsn);
    await migrator.connect();
    await migrator.close();
    this.admin = new pg.Pool({ connectionString: this.dsn });
  }

  async truncate(): Promise<void> {
    await this.admin!.query("truncate cairnq_tasks, cairnq_task_keys");
  }

  async client(opts: ClientOptions = {}): Promise<CairnQ> {
    const c = CairnQ.postgres(this.dsn, opts);
    await c.connect();
    this.open.push(c);
    return c;
  }

  worker(opts: WorkerOptions & { queues?: string[] } = {}): Worker {
    return Worker.postgres(this.dsn, opts);
  }

  async closeClients(): Promise<void> {
    const open = this.open;
    this.open = [];
    await Promise.all(open.map((c) => c.close().catch(() => {})));
  }

  async dropDatabase(): Promise<void> {
    await this.admin?.end();
    const maintenance = new pg.Pool({ connectionString: PG_DSN });
    await maintenance.query(`drop database if exists ${dbNameFor(this.title)}`).catch(() => {});
    await maintenance.end();
  }
}

/** A file of its own per test — the cheapest possible empty database. */
class SqliteBackend implements Backend {
  readonly name = "sqlite" as const;
  private path = "";
  private open: CairnQ[] = [];

  fresh(): void {
    this.path = freshDbPath();
  }

  async client(opts: ClientOptions = {}): Promise<CairnQ> {
    const c = CairnQ.sqlite(this.path, opts);
    await c.connect();
    this.open.push(c);
    return c;
  }

  worker(opts: WorkerOptions & { queues?: string[] } = {}): Worker {
    return Worker.sqlite(this.path, opts);
  }

  async closeClients(): Promise<void> {
    const open = this.open;
    this.open = [];
    await Promise.all(open.map((c) => c.close().catch(() => {})));
  }
}

export interface BackendOptions {
  /**
   * The backends this suite runs on. Omit for both.
   *
   * Naming one is a claim that the other would prove nothing, and `because` is
   * where that claim is argued — group commit is a SQLite mechanism, the
   * schema guard is a Postgres one. An unexplained restriction is how a gap
   * comes to look like a decision.
   */
  only?: BackendName[];
  because?: string;
}

/**
 * `describe`, once per backend, with a fresh empty database per test.
 *
 * The Postgres arm skips without a DSN rather than failing, so `pnpm test`
 * works on a laptop. CI's `postgres` job runs the WHOLE suite with a DSN set,
 * which is what makes the declaration true — for a while it named individual
 * files instead, so every arm declared here was written and never executed.
 */
export function describeBackends(
  title: string,
  fn: (backend: Backend) => void,
  opts: BackendOptions = {},
): void {
  const names = opts.only ?? (["sqlite", "postgres"] as const);
  for (const name of names) {
    if (name === "sqlite") {
      const backend = new SqliteBackend();
      describe(`${title} [sqlite]`, () => {
        beforeEach(() => backend.fresh());
        afterEach(() => backend.closeClients());
        fn(backend);
      });
    } else {
      const backend = new PostgresBackend(title);
      const suite = PG_DSN ? describe : describe.skip;
      suite(`${title} [postgres]`, () => {
        beforeAll(() => backend.createDatabase());
        afterAll(() => backend.dropDatabase());
        beforeEach(() => backend.truncate());
        afterEach(() => backend.closeClients());
        fn(backend);
      });
    }
  }
}
