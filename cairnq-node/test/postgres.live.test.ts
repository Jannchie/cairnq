import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import pg from "pg";

import { CairnQ, LostLease, PostgresStore, SchemaMismatch, Worker } from "../src/index.js";
import { allTerminal, sleep, waitFor } from "./helpers.js";

// Live Postgres smoke. Skipped unless CAIRNQ_TEST_PG_DSN is set — CI provides a
// `postgres` service; locally you can point it at any throwaway database. It runs
// the paths static review can't prove out: real pg type inference on NULL/jsonb
// params, FOR UPDATE SKIP LOCKED under genuine concurrency, the JSON round-trip,
// DB-clock lease recovery, and the *_by_key transactions.
const DSN = process.env.CAIRNQ_TEST_PG_DSN;
const suite = DSN ? describe : describe.skip;

// This suite runs in a database of its own, created once in beforeAll: vitest
// runs test FILES in parallel, and sharing one database with the conformance
// file means its truncates race these tests' rows (tests within this file stay
// sequential, so they can share it safely).
const LIVE_DSN = DSN
  ? Object.assign(new URL(DSN), { pathname: "/cairnq_live_test" }).toString()
  : "";

suite("postgres live", () => {
  let client: CairnQ;
  let admin: pg.Pool;

  beforeAll(async () => {
    const maintenance = new pg.Pool({ connectionString: DSN });
    await maintenance.query("create database cairnq_live_test").catch(() => {});
    await maintenance.end();
  });

  beforeEach(async () => {
    client = CairnQ.postgres(LIVE_DSN);
    await client.connect(); // applies migrations (idempotent)
    admin = new pg.Pool({ connectionString: LIVE_DSN });
    await admin.query("truncate cairnq_tasks, cairnq_task_keys");
  });

  afterEach(async () => {
    await admin.end();
    await client.close();
  });

  it("round-trips a null/nested/unicode payload and jsonb result", async () => {
    const payload = { a: null, nested: { x: [1, 2, 3] }, u: "café — 日本語", empty: {} };
    const t = await client.submit("job", payload);
    expect((await client.get(t.id))?.payload).toEqual(payload);

    const [c] = await client.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 5000 });
    expect(c.id).toBe(t.id);
    await client.store.succeed({ taskId: t.id, workerId: "w1", result: { out: [null, { k: "ü" }] } });
    const done = await client.get(t.id);
    expect(done?.status).toBe("succeeded");
    expect(done?.result).toEqual({ out: [null, { k: "ü" }] });
  });

  it("binds NULL params correctly: progress(null message) + succeed(null result)", async () => {
    const t = await client.submit("job", {});
    await client.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 5000 });
    const p = await client.store.progress({ taskId: t.id, workerId: "w1", progress: 0.5, message: null });
    expect(p.progress).toBe(0.5);
    await client.store.succeed({ taskId: t.id, workerId: "w1", result: null });
    const done = await client.get(t.id);
    expect(done?.status).toBe("succeeded");
    expect(done?.result).toBeNull();
  });

  it("recovers an expired lease using the DB clock", async () => {
    const t = await client.submit("job", {}, { maxAttempts: 3 });
    await client.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 150 });
    await new Promise((r) => setTimeout(r, 300));
    const [c2] = await client.store.claim({ queues: ["default"], workerId: "w2", leaseMs: 5000 });
    expect(c2.id).toBe(t.id);
    expect(c2.attempt).toBe(2);
  });

  it("never double-dispatches under genuinely concurrent claims (SKIP LOCKED)", async () => {
    const n = 12;
    for (let i = 0; i < n; i++) await client.submit("job", { i });
    // pg is async + pooled, so these N+6 claims truly race — the SQLite version of
    // this test serializes and can't actually exercise SKIP LOCKED. This can.
    const batches = await Promise.all(
      Array.from({ length: n + 6 }, (_, k) =>
        client.store.claim({ queues: ["default"], workerId: `w${k}`, leaseMs: 5000, limit: 1 }),
      ),
    );
    const ids = batches.flat().map((t) => t.id);
    expect(ids.length).toBe(n);
    expect(new Set(ids).size).toBe(n);
  });

  it("cancel_by_key / retry_by_key go through the key table transactionally", async () => {
    await client.submit("job", {}, { key: "K", maxAttempts: 1 });
    expect((await client.cancelByKey("K"))?.status).toBe("canceled");
    const retried = await client.retryByKey("K", { resetAttempt: true });
    expect(retried?.status).toBe("queued");
    expect(retried?.attempt).toBe(0);
    expect(await client.cancelByKey("missing")).toBeNull();
  });

  it("keeps reuse idempotent under genuinely concurrent same-key submits", async () => {
    // Regression: without lock_key.sql these race through READ COMMITTED (no
    // key row to lock yet), every submit sees "no existing task", and one key
    // ends up with several live tasks. The pool gives each submit its own
    // connection, so they truly race.
    const results = await Promise.all(
      Array.from({ length: 8 }, () => client.submit("job", {}, { key: "K", conflict: "reuse" })),
    );
    expect(new Set(results.map((t) => t.id)).size).toBe(1);
    const live = await client.list({ name: "job", status: "queued" });
    expect(live.length).toBe(1);
  });

  it("keeps reuse idempotent when the key's last task already finished", async () => {
    // The same race one step later, and the one that actually costs money: the
    // key points at a terminal task, so every racer wants to start a fresh one.
    // The advisory lock has to serialize them into one insert plus seven reuses
    // of it — otherwise the second submit cancels the first's brand-new task and
    // the work runs twice against a caller who gets `canceled` back.
    const first = await client.submit("job", {}, { key: "T" });
    await client.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 5_000 });
    await client.store.succeed({ taskId: first.id, workerId: "w1", result: { n: 1 } });

    const results = await Promise.all(
      Array.from({ length: 8 }, () => client.submit("job", {}, { key: "T", conflict: "reuse" })),
    );
    const ids = new Set(results.map((t) => t.id));
    expect(ids.size).toBe(1);
    expect(ids.has(first.id)).toBe(false);
    expect(results.every((t) => t.status === "queued")).toBe(true);
  });

  it("rejects a non-owner worker write with LostLease", async () => {
    const t = await client.submit("job", {});
    await client.store.claim({ queues: ["default"], workerId: "owner", leaseMs: 5000 });
    await expect(
      client.store.complete({ taskId: t.id, workerId: "intruder", result: {} }),
    ).rejects.toBeInstanceOf(LostLease);
  });

  it("wakes the worker and the waiter by NOTIFY, beating the poll floor", async () => {
    // Poll intervals are set far above the assertion, so finishing in time is
    // only possible if LISTEN/NOTIFY cut both sleeps short: the worker's idle
    // (claim poll 5s) and call's wait poll (4s).
    const store = new PostgresStore(LIVE_DSN);
    await store.connect();
    await sleep(500); // let the LISTEN connections warm up
    const worker = new Worker(store, ["default"], { pollIntervalMs: 5_000 });
    worker.task("ping", () => ({ pong: true }));
    try {
      const t0 = Date.now();
      const result = await worker.background(async () => {
        await sleep(300); // park the worker in its idle sleep first
        return client.call("ping", {}, { waitTimeoutMs: 8_000, pollMs: 4_000 });
      });
      expect(result).toEqual({ pong: true });
      expect(Date.now() - t0).toBeLessThan(3_000);
    } finally {
      await store.close();
    }
  }, 15_000);

  it("runs a worker end-to-end over postgres", async () => {
    const worker = Worker.postgres(LIVE_DSN, { queues: ["default"], pollIntervalMs: 50 });
    worker.task("sum", async (_ctx, p) => ({ sum: p.a + p.b }));
    const result = await worker.background(() =>
      client.call("sum", { a: 2, b: 3 }, { waitTimeoutMs: 10_000, pollMs: 50 }),
    );
    expect(result).toEqual({ sum: 5 });
  });

  it("delivers a batch end-to-end over postgres", async () => {
    // Exercises heartbeat_batch.sql's Postgres form — `= any(:ids::text[])` with
    // a real array bind, which no SQLite run can prove out — and the
    // settle-the-rest contract over the DB clock rather than a supplied nowMs.
    const worker = Worker.postgres(LIVE_DSN, {
      queues: ["default"],
      pollIntervalMs: 50,
      concurrency: 8,
      leaseMs: 400,
      heartbeatIntervalMs: 60,
      retryBackoffMs: 0,
    });
    const seen: number[] = [];

    worker.task("embed", { batch: 8 }, async (items) => {
      seen.push(items.length);
      // Outlive the lease, so the batch heartbeat is what keeps these claimed.
      await sleep(700);
      for (const item of items) {
        if (item.payload.n === 1) await item.fail("odd one out", { retryable: false });
      }
      return Object.fromEntries(items.map((i) => [i.taskId, { n: i.payload.n }]));
    });

    const ids: string[] = [];
    for (let n = 0; n < 4; n++) {
      ids.push((await client.submit("embed", { n }, { maxAttempts: 1 })).id);
    }

    await worker.background(async () => {
      await waitFor(() => allTerminal(client, ids), 10_000);
    });

    const tasks = new Map(
      (await Promise.all(ids.map((id) => client.get(id)))).map((t) => [(t!.payload as any).n, t!]),
    );
    expect(seen).toEqual([4]);
    expect(tasks.get(1)!.status).toBe("failed");
    expect((tasks.get(1)!.error as any).message).toBe("odd one out");
    expect([0, 2, 3].map((n) => tasks.get(n)!.status)).toEqual([
      "succeeded",
      "succeeded",
      "succeeded",
    ]);
    // Never redelivered despite outliving the lease: one attempt each.
    expect([...tasks.values()].every((t) => t.attempt === 1)).toBe(true);
  }, 20_000);

  // ------------------------------------------------------------ split brain
  // The one failure a fake cannot prove: `create table if not exists` really
  // does let a second, parallel installation come up beside the first, against
  // a real server, with the version check really passing on both sides.
  describe("schema", () => {
    it("puts the tables in the schema it was given, and joins them again", async () => {
      const scoped = CairnQ.postgres(LIVE_DSN, { schema: "cairnq_isolated" });
      try {
        await scoped.connect(); // creates the schema, migrates into it
        const t = await scoped.submit("job", { n: 1 });
        // The default-schema client shares the DSN and sees nothing of it.
        expect(await client.get(t.id)).toBeNull();
        // A second connection naming the same schema picks it straight up.
        const rejoin = CairnQ.postgres(LIVE_DSN, { schema: "cairnq_isolated" });
        try {
          expect((await rejoin.get(t.id))?.payload).toEqual({ n: 1 });
        } finally {
          await rejoin.close();
        }
      } finally {
        await scoped.close();
        await admin.query("drop schema if exists cairnq_isolated cascade");
      }
    });

    it("refuses to come up in a schema beside an installation that already exists", async () => {
      // `client` (beforeEach) already migrated into the default schema, so a
      // connection landing anywhere else is about to split the deployment in two.
      await admin.query("create schema if not exists cairnq_elsewhere");
      const other = CairnQ.postgres(LIVE_DSN, { schema: "cairnq_elsewhere" });
      try {
        // Explicit: allowed, and it is the confirmation the guard asks for.
        await other.connect();
      } finally {
        await other.close();
      }
      // Implicit, via the DSN — the shape a mismatched pair of SDKs actually
      // takes. Nothing about it is distinguishable downstream, so it has to fail
      // here or not at all.
      const url = new URL(LIVE_DSN);
      url.searchParams.set("options", "-c search_path=cairnq_elsewhere2");
      await admin.query("create schema if not exists cairnq_elsewhere2");
      const implicit = CairnQ.postgres(url.toString());
      try {
        await expect(implicit.connect()).rejects.toBeInstanceOf(SchemaMismatch);
      } finally {
        await implicit.close();
        await admin.query("drop schema if exists cairnq_elsewhere cascade");
        await admin.query("drop schema if exists cairnq_elsewhere2 cascade");
      }
    });
  });
});
