import { describe, expect, it } from "vitest";

import {
  CairnQ,
  createPoolExecutor,
  ListenUnavailable,
  LostLease,
  type PgExecutor,
  type PgSession,
  PostgresStore,
  type Row,
  SchemaMismatch,
  TaskContext,
} from "../src/index.js";

// The store's half of the PgExecutor contract, exercised against a recording
// fake. The SQL itself needs a real Postgres (see postgres.live.test.ts and the
// conformance suite); what is checked here is what the store ASKS the executor
// to do — which is exactly what an adapter over another driver has to satisfy,
// and what a refactor of the connection seam could silently break.

/** Comments and runs of whitespace, so a recorded call is one readable line. */
const COMMENT_OR_SPACE = /--[^\n]*|\s+/g;

interface Recorder {
  executor: PgExecutor;
  calls: string[];
  closed: number;
}

function fakeExecutor(over: Partial<PgExecutor> = {}): Recorder {
  const calls: string[] = [];
  const rec = { calls, closed: 0 } as Recorder;
  const session: PgSession = {
    async query(text: string): Promise<Row[]> {
      // Tagged, not truncated: every protocol statement opens with a comment
      // block, so the first N characters identify none of them.
      calls.push(
        /current_schema\(\)/.test(text)
          ? "query:installations"
          : `query:${text.replace(COMMENT_OR_SPACE, " ").trim().slice(0, 48)}`,
      );
      // The only read doConnect makes: cairnq_meta's protocol_version. Anything
      // else in the connect path is a migration bookkeeping SELECT, and an empty
      // result there means "not applied yet".
      if (/protocol_version/.test(text) && /select/i.test(text)) return [{ value: "1" }];
      return [];
    },
    async exec(sql: string): Promise<void> {
      calls.push(`exec:${sql.replace(/\s+/g, " ").trim().slice(0, 48)}`);
    },
  };
  rec.executor = {
    query: session.query,
    exec: session.exec,
    async tx<T>(fn: (s: PgSession) => Promise<T>): Promise<T> {
      calls.push("tx:begin");
      const out = await fn(session);
      calls.push("tx:commit");
      return out;
    },
    async close(): Promise<void> {
      rec.closed++;
    },
    ...over,
  };
  return rec;
}

describe("PostgresStore over an injected executor", () => {
  it("runs every migration inside a transaction that takes the ledger lock first", async () => {
    const rec = fakeExecutor();
    const store = new PostgresStore(rec.executor);
    await store.connect();

    // The ledger table itself is created outside any transaction...
    const ledger = rec.calls.findIndex((c) =>
      c.includes("create table if not exists cairnq_migrations"),
    );
    expect(ledger).toBeGreaterThanOrEqual(0);
    // ...and the split-brain check comes before it, while there is still nothing
    // of ours in this schema to find.
    expect(rec.calls.slice(0, ledger)).toContain("query:installations");
    // ...and then every migration is check-and-apply under the lock. Without the
    // lock inside the transaction, two processes cold-starting together both see
    // a migration as unapplied and both run it.
    const begins = rec.calls.filter((c) => c === "tx:begin").length;
    expect(begins).toBeGreaterThan(0);
    expect(rec.calls.filter((c) => c === "tx:commit")).toHaveLength(begins);
    expect(rec.calls.indexOf("tx:begin")).toBeGreaterThan(ledger);
    const firstBegin = rec.calls.indexOf("tx:begin");
    expect(rec.calls[firstBegin + 1]).toContain("lock table cairnq_migrations in exclusive mode");
  });

  it("never closes an executor it was handed", async () => {
    const rec = fakeExecutor();
    const store = new PostgresStore(rec.executor);
    await store.connect();
    await store.close();
    // The caller's other work — an ORM sharing this pool — has to survive cairnq
    // shutting down.
    expect(rec.closed).toBe(0);
  });

  it("refuses to publish a store whose protocol version disagrees", async () => {
    const rec = fakeExecutor();
    rec.executor.query = async (text: string) =>
      /protocol_version/.test(text) && /select/i.test(text) ? [{ value: "999" }] : [];
    const store = new PostgresStore(rec.executor);
    await expect(store.connect()).rejects.toThrow(/protocol_version=999/);
    expect(rec.closed).toBe(0); // still not ours to close
  });

  it("falls back to polling when the executor cannot listen at all", async () => {
    const rec = fakeExecutor({ listen: undefined });
    const store = new PostgresStore(rec.executor);
    await store.connect();
    // No push channel: the wake must still return (on the timeout), because
    // notifications only shorten a poll sleep and are never required.
    const started = Date.now();
    await store.claimWake(["default"], 20);
    expect(Date.now() - started).toBeGreaterThanOrEqual(15);
    await store.close();
  });

  it("stops retrying LISTEN once the executor calls it deterministically unavailable", async () => {
    let attempts = 0;
    const rec = fakeExecutor({
      async listen() {
        attempts++;
        throw new ListenUnavailable("transaction-mode pooler");
      },
    });
    const store = new PostgresStore(rec.executor);
    await store.connect();
    for (let i = 0; i < 3; i++) await store.claimWake(["default"], 5);
    // A pooler that refuses LISTEN refuses it every time; reconnecting from the
    // poll loop would be a loop that cannot succeed.
    expect(attempts).toBe(1);
    await store.close();
  });

  it("delivers a notification to a waiting claimWake", async () => {
    let notify: ((channel: string, payload: string) => void) | null = null;
    const rec = fakeExecutor({
      async listen(_channels, onNotify) {
        notify = onNotify as typeof notify;
        return () => {};
      },
    });
    const store = new PostgresStore(rec.executor);
    await store.connect();
    await store.claimWake(["default"], 1); // establishes the subscription
    await new Promise((r) => setTimeout(r, 5));
    expect(notify).not.toBeNull();

    const wake = store.claimWake(["default"], 5_000);
    await new Promise((r) => setTimeout(r, 5));
    notify!("cairnq_queued", "default");
    await wake; // resolves on the notification, well inside the 5s timeout
    await store.close();
  });
});

// ---------------------------------------------------------------- shared txn

/** A fake whose `complete` can be made to match no row, i.e. a lost lease. */
function settlementFake(opts: { completeMatches: boolean }) {
  const calls: string[] = [];
  let completeValues: readonly unknown[] = [];
  let rolledBack = false;
  const session: PgSession = {
    async query(text: string, values: readonly unknown[]): Promise<Row[]> {
      const tag = /update cairnq_tasks/.test(text) && /succeeded/.test(text) ? "complete" : "other";
      if (tag === "complete") {
        calls.push("complete");
        completeValues = values;
        return opts.completeMatches ? [row()] : [];
      }
      if (/protocol_version/.test(text) && /select/i.test(text)) return [{ value: "1" }];
      calls.push("query");
      return [];
    },
    async exec(): Promise<void> {
      calls.push("exec");
    },
  };
  const executor: PgExecutor = {
    query: session.query,
    exec: session.exec,
    async tx<T>(fn: (s: PgSession) => Promise<T>): Promise<T> {
      calls.push("BEGIN");
      try {
        const out = await fn(session);
        calls.push("COMMIT");
        return out;
      } catch (e) {
        rolledBack = true;
        calls.push("ROLLBACK");
        throw e;
      }
    },
    async close() {},
  };
  return {
    executor,
    calls,
    get completeValues() { return completeValues; },
    get rolledBack() { return rolledBack; },
  };
}

function row(): Row {
  const now = 1_700_000_000_000;
  return {
    id: "t1", name: "render", queue: "default", status: "succeeded",
    payload: {}, metadata: {}, result: null, error: null,
    progress: null, message: null, attempt: 1, max_attempts: 3, priority: 0,
    worker_id: "w1", lease_until_ms: null, run_at_ms: now,
    cancel_requested_at_ms: null, parent_id: null, root_id: null,
    correlation_id: null, created_at_ms: now, updated_at_ms: now, completed_at_ms: now,
  };
}

function context(store: PostgresStore): TaskContext {
  return new TaskContext(store, row() as never, "w1", 30_000);
}

describe("ctx.succeedIn — settlement and the caller's writes in one transaction", () => {
  it("commits the caller's writes and the settlement together, in that order", async () => {
    const fake = settlementFake({ completeMatches: true });
    const store = new PostgresStore(fake.executor);
    await store.connect();
    fake.calls.length = 0;

    const task = await context(store).succeedIn(async (session) => {
      await session.query("insert into visual_pages (id) values ($1)", ["p1"]);
      return { pages: 1 };
    });

    expect(task?.status).toBe("succeeded");
    // One transaction; the caller's write inside it; the settlement last, so a
    // lease lost at the end takes the caller's write down with it.
    expect(fake.calls[0]).toBe("BEGIN");
    expect(fake.calls.at(-1)).toBe("COMMIT");
    expect(fake.calls.indexOf("query")).toBeLessThan(fake.calls.indexOf("complete"));
  });

  it("makes what the callback returns the task's result", async () => {
    const fake = settlementFake({ completeMatches: true });
    const store = new PostgresStore(fake.executor);
    await store.connect();
    fake.calls.length = 0;
    await context(store).succeedIn(async () => ({ pages: 7 }));
    // Bound as JSON text, the same marshalling ctx.succeed(result) does.
    const asJson = fake.completeValues.find((v) => typeof v === "string" && v.startsWith("{"));
    expect(JSON.parse(asJson as string)).toEqual({ pages: 7 });
  });

  it("rolls the caller's writes back when the lease turned out to be gone", async () => {
    const fake = settlementFake({ completeMatches: false });
    const store = new PostgresStore(fake.executor);
    await store.connect();
    fake.calls.length = 0;

    await expect(
      context(store).succeedIn(async (session) => {
        await session.query("insert into visual_pages (id) values ($1)", ["p1"]);
        return null;
      }),
    ).rejects.toBeInstanceOf(LostLease);

    // The whole point: no ordering exists in which those pages are durable and
    // the task still reads as running.
    expect(fake.rolledBack).toBe(true);
    expect(fake.calls).not.toContain("COMMIT");
  });

  it("does not settle twice", async () => {
    const fake = settlementFake({ completeMatches: true });
    const store = new PostgresStore(fake.executor);
    await store.connect();
    const ctx = context(store);
    await ctx.succeedIn(async () => null);
    expect(await ctx.succeedIn(async () => null)).toBeNull();
  });

  it("tells a SQLite store plainly that it cannot do this", async () => {
    const store = CairnQ.sqlite(":memory:");
    await expect(
      store.store.completeIn({ taskId: "t1", workerId: "w1" }, async () => null),
    ).rejects.toThrow(/cannot share a transaction/);
    await store.close();
  });
});

describe("schema option", () => {
  it("refuses a schema name that could rewrite the statement it is quoted into", async () => {
    // cairnq interpolates the schema (a `set search_path` takes no parameters),
    // so the identifier is checked rather than trusted.
    for (const bad of ['pub"lic', "a; drop table cairnq_tasks", "1st", "with space", ""]) {
      await expect(createPoolExecutor("postgres://x/y", { schema: bad })).rejects.toThrow(
        /plain identifier/,
      );
    }
  });

  it("lets an ordinary identifier through the guard", async () => {
    // A schema makes the executor connect (it creates the schema up front), so
    // an unreachable DSN is expected to fail — just not on the name. Whether the
    // search_path then actually takes effect needs live Postgres.
    const err = await createPoolExecutor("postgres://u@127.0.0.1:1/x", {
      schema: "cairnq_2",
    }).then(
      () => null,
      (e: Error) => e,
    );
    expect(err).not.toBeNull();
    expect(err!.message).not.toMatch(/plain identifier/);
  });
});

// ------------------------------------------------------------ split brain

/** An executor reporting a database that already holds cairnq somewhere. */
function inSchema(current: string | null, installations: string[]): PgExecutor {
  const query = async (text: string): Promise<Row[]> => {
    if (/current_schema\(\)/.test(text)) {
      return [{ current_schema: current, installations }];
    }
    return /protocol_version/.test(text) && /select/i.test(text) ? [{ value: "1" }] : [];
  };
  const session: PgSession = { query, exec: async () => {} };
  return {
    query,
    exec: session.exec,
    tx: async (fn) => fn(session),
    close: async () => {},
  };
}

describe("split-brain guard", () => {
  it("refuses to build a second installation beside one that already exists", async () => {
    // The reported failure: the TS side migrated into `cairnq`, the other end
    // defaults to `public`, both succeed, and no task ever crosses. Nothing
    // downstream can see it — protocol_version reads from whichever cairnq_meta
    // the connection found, so that check passes on both sides.
    const store = new PostgresStore(inSchema("public", ["cairnq"]));
    await expect(store.connect()).rejects.toBeInstanceOf(SchemaMismatch);
    await expect(store.connect()).rejects.toThrow(/"cairnq".*"public"/s);
  });

  it("is quiet when this connection joins the installation that exists", async () => {
    const store = new PostgresStore(inSchema("cairnq", ["cairnq"]));
    await expect(store.connect()).resolves.toBeUndefined();
  });

  it("is quiet on a database with no cairnq in it yet", async () => {
    const store = new PostgresStore(inSchema("public", []));
    await expect(store.connect()).resolves.toBeUndefined();
  });

  it("lets an explicit schema through — two deployments in one database", async () => {
    // Saying it is both the fix and the confirmation: a second application
    // legitimately running its own cairnq names its schema, and that is exactly
    // what the error tells the ambiguous case to do.
    const store = new PostgresStore(inSchema("app_b", ["app_a"]), { schema: "app_b" });
    await expect(store.connect()).resolves.toBeUndefined();
  });

  it("checks an explicit schema against where the connection actually landed", async () => {
    // The only way an injected executor can state the expectation at all: cairnq
    // did not build that connection and cannot set its search_path.
    const store = new PostgresStore(inSchema("public", ["public"]), { schema: "cairnq" });
    await expect(store.connect()).rejects.toThrow(/configured for schema "cairnq"/);
  });

  it("stays out of the way when the search_path resolves to nothing", async () => {
    // Nothing to compare against, and the migrations are about to fail with a
    // message that names the real problem.
    const store = new PostgresStore(inSchema(null, ["cairnq"]));
    await expect(store.connect()).resolves.toBeUndefined();
  });
});
