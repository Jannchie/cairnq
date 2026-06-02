import { afterEach, beforeEach, describe, expect, it } from "vitest";
import pg from "pg";

import { CairnQ, LostLease, Worker } from "../src/index.js";

// Live Postgres smoke. Skipped unless CAIRNQ_TEST_PG_DSN is set — CI provides a
// `postgres` service; locally you can point it at any throwaway database. It runs
// the paths static review can't prove out: real pg type inference on NULL/jsonb
// params, FOR UPDATE SKIP LOCKED under genuine concurrency, the JSON round-trip,
// DB-clock lease recovery, and the *_by_key transactions.
const DSN = process.env.CAIRNQ_TEST_PG_DSN;
const suite = DSN ? describe : describe.skip;

suite("postgres live", () => {
  let client: CairnQ;
  let admin: pg.Pool;

  beforeEach(async () => {
    client = CairnQ.postgres(DSN!);
    await client.connect(); // applies migrations (idempotent)
    admin = new pg.Pool({ connectionString: DSN });
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

  it("rejects a non-owner worker write with LostLease", async () => {
    const t = await client.submit("job", {});
    await client.store.claim({ queues: ["default"], workerId: "owner", leaseMs: 5000 });
    await expect(
      client.store.complete({ taskId: t.id, workerId: "intruder", result: {} }),
    ).rejects.toBeInstanceOf(LostLease);
  });

  it("runs a worker end-to-end over postgres", async () => {
    const worker = Worker.postgres(DSN!, { queues: ["default"], pollIntervalMs: 50 });
    worker.task("sum", async (_ctx, p) => ({ sum: p.a + p.b }));
    const result = await worker.background(() =>
      client.call("sum", { a: 2, b: 3 }, { waitTimeoutMs: 10_000, pollMs: 50 }),
    );
    expect(result).toEqual({ sum: 5 });
  });
});
