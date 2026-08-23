// Group commit: writes that are already waiting on the store's lock share one
// transaction.
//
// A protocol write costs microseconds to execute and a WAL commit to durably
// land, so concurrent writers spend nearly all their time on commits they could
// have shared. Nothing waits to form a batch — a flusher takes whatever arrived
// while the previous one held the lock — so the trade is not latency but
// atomicity: two callers' writes now land together or not at all, which under
// at-least-once is a redelivery rather than a lost task.
//
// What each test here is really guarding is a way to get this wrong quietly:
// results crossing between callers, one bad write poisoning its neighbours, a
// write arriving mid-batch and being stranded, and reads being dragged into the
// write transaction.
import { describe, expect, it } from "vitest";
import Database from "better-sqlite3";
import { statSync } from "node:fs";

import { CairnQ, SQLiteStore, StoreClosed } from "../src/index.js";
import { freshDbPath } from "./helpers.js";

const WORKER = "w1";

/**
 * Frames written to the WAL so far.
 *
 * The WAL is a 32-byte header plus fixed-size frames, and every commit appends at
 * least one — so this counts commits from the outside, without the store having to
 * report on itself. Repeated writes to one small table touch one page, which is
 * what makes the count track transactions rather than data volume.
 */
function walFrames(path: string, pageSize: number): number {
  const size = statSync(`${path}-wal`).size;
  return size <= 32 ? 0 : (size - 32) / (24 + pageSize);
}

function pageSizeOf(path: string): number {
  const db = new Database(path);
  try {
    return db.pragma("page_size", { simple: true }) as number;
  } finally {
    db.close();
  }
}

/** `n` claimed tasks, ready to be finalized. */
async function claimed(client: CairnQ, store: SQLiteStore, n: number): Promise<string[]> {
  await Promise.all(Array.from({ length: n }, (_, i) => client.submit("job", { i })));
  const tasks = await store.claim({
    queues: ["default"],
    workerId: WORKER,
    leaseMs: 600_000,
    limit: n,
    names: ["job"],
  });
  expect(tasks.length).toBe(n);
  return tasks.map((t) => t.id);
}

describe("group commit", () => {
  it("commits concurrent writes together and sequential writes separately", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    const store = new SQLiteStore(path);
    await client.connect();
    await store.connect();
    try {
      const pageSize = pageSizeOf(path);
      const N = 32;

      const serialIds = await claimed(client, store, N);
      const beforeSerial = walFrames(path, pageSize);
      for (const id of serialIds) {
        await store.complete({ taskId: id, workerId: WORKER, result: {} });
      }
      const serialFrames = walFrames(path, pageSize) - beforeSerial;

      const batchIds = await claimed(client, store, N);
      const beforeBatch = walFrames(path, pageSize);
      await Promise.all(
        batchIds.map((id) => store.complete({ taskId: id, workerId: WORKER, result: {} })),
      );
      const batchFrames = walFrames(path, pageSize) - beforeBatch;

      // Awaited one at a time, nothing is ever waiting, so each write is its own
      // transaction — the uncontended path has to stay that cheap.
      expect(serialFrames).toBeGreaterThanOrEqual(N);
      // Issued together, they share one. Not asserted as exactly one commit: the
      // first writer takes the lock before the rest have been issued, so a real
      // run is a small handful of batches rather than a single one.
      expect(batchFrames).toBeLessThan(serialFrames / 4);
    } finally {
      await client.close();
      await store.close();
    }
  });

  it("gives each concurrent writer its own rows", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    const store = new SQLiteStore(path);
    await client.connect();
    await store.connect();
    try {
      const ids = await claimed(client, store, 16);
      // A distinct result per task: a batch that merged or misordered its rows
      // would hand somebody else's back.
      const done = await Promise.all(
        ids.map((id, i) => store.complete({ taskId: id, workerId: WORKER, result: { i } })),
      );
      expect(done.map((t) => t.id)).toEqual(ids);
      expect(done.map((t) => t.result)).toEqual(ids.map((_, i) => ({ i })));
    } finally {
      await client.close();
      await store.close();
    }
  });

  it("fails only the writer whose own write failed", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    const store = new SQLiteStore(path);
    await client.connect();
    await store.connect();
    try {
      const ids = await claimed(client, store, 8);
      // One finalize by a worker that does not own the lease: its statement matches
      // no row, which is a lost lease. Sharing a transaction must not spread that
      // to the seven around it, nor roll their writes back.
      const outcomes = await Promise.allSettled([
        ...ids.slice(0, 4).map((id) => store.complete({ taskId: id, workerId: WORKER, result: {} })),
        store.complete({ taskId: ids[4], workerId: "someone-else", result: {} }),
        ...ids.slice(5).map((id) => store.complete({ taskId: id, workerId: WORKER, result: {} })),
      ]);
      expect(outcomes.filter((o) => o.status === "rejected").length).toBe(1);
      expect(outcomes[4].status).toBe("rejected");

      const rows = await Promise.all(ids.map((id) => client.get(id)));
      expect(rows.map((t) => t?.status)).toEqual([
        "succeeded",
        "succeeded",
        "succeeded",
        "succeeded",
        "running",
        "succeeded",
        "succeeded",
        "succeeded",
      ]);
    } finally {
      await client.close();
      await store.close();
    }
  });

  it("picks up a write issued while a batch is in flight", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    const store = new SQLiteStore(path);
    await client.connect();
    await store.connect();
    try {
      const ids = await claimed(client, store, 12);
      // The window a naive flusher strands a write in: issued from the callback of
      // an earlier one, so it arrives after the batch was taken but before the
      // flusher has finished with it. If nothing re-checks, this never resolves and
      // the test times out rather than failing an assertion.
      const first = ids.slice(0, 6);
      const chased = ids.slice(6);
      await Promise.all(
        first.map((id, i) =>
          store
            .complete({ taskId: id, workerId: WORKER, result: {} })
            .then(() => store.complete({ taskId: chased[i], workerId: WORKER, result: {} })),
        ),
      );
      const rows = await Promise.all(ids.map((id) => client.get(id)));
      expect(rows.every((t) => t?.status === "succeeded")).toBe(true);
    } finally {
      await client.close();
      await store.close();
    }
  });

  it("keeps reads out of the write transaction", async () => {
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    // Hold the write lock from a connection nothing serializes against — a
    // competing process, as far as the store is concerned.
    const blocker = new Database(path);
    blocker.pragma("busy_timeout = 0");
    blocker.exec("BEGIN IMMEDIATE");
    try {
      // Reads must still go through. If a read were batched with the writes it
      // would need the write lock this blocker is holding, and an idle worker's
      // poll would stall behind unrelated writers — the whole point of the
      // read-only claim probe.
      expect(await client.get("absent")).toBeNull();
      expect(await client.list({ limit: 1 })).toEqual([]);
      expect(await client.queueDepth("default", 5)).toBe(5);
    } finally {
      blocker.exec("ROLLBACK");
      blocker.close();
      await client.close();
    }
  });

  it("lands the writes already queued before closing the connection", async () => {
    // close() used to drop the connection while a group commit still held
    // writes their callers were awaiting: the flusher's next prepare ran against
    // a null `db` and the submit rejected with a TypeError about
    // `Cannot read properties of null`, losing a write that had been accepted.
    // It now waits for the flusher — as the Python twin already did.
    const path = freshDbPath();
    const client = CairnQ.sqlite(path);
    await client.connect();
    // Issued without awaiting, so they are still in `pending` when close lands.
    const inFlight = [
      client.submit("a", { i: 0 }),
      client.submit("a", { i: 1 }),
      client.submit("a", { i: 2 }),
    ];
    await client.close();
    const landed = await Promise.all(inFlight);
    expect(landed).toHaveLength(3);

    // And they are durable, not merely resolved: a fresh handle sees all three.
    const reader = CairnQ.sqlite(path);
    try {
      const rows = await reader.list({ name: "a" });
      expect(rows.map((r) => (r.payload as { i: number }).i).sort()).toEqual([0, 1, 2]);
    } finally {
      await reader.close();
    }
  });

  it("turns away work that arrives while it is closing", async () => {
    // The drain has to be finite: without a barrier a write landing between the
    // flusher finishing and the connection closing starts a fresh flusher
    // against a connection about to go away. Anything arriving now gets a typed
    // StoreClosed instead.
    // Driven through the store rather than CairnQ: the handle awaits its
    // retention sweeper first, which yields a turn — long enough for a submit
    // issued "during" close to actually arrive before it, and be drained by the
    // case above. The barrier is the store's, so test it there.
    const store = new SQLiteStore(freshDbPath());
    await store.connect();
    const closing = store.close();
    await expect(store.submit({ name: "a", payload: {} })).rejects.toThrow(StoreClosed);
    await closing;
    // Closing does not retire the store: a file-backed one reopens on next use.
    const t = await store.submit({ name: "a", payload: {} });
    expect(t.id).toBeTruthy();
    await store.close();
  });

  it("refuses to reopen an in-memory database it already closed", async () => {
    // A file store reopens onto its tasks; an in-memory one would reopen onto
    // nothing, because its contents lived in the connection. That used to
    // succeed silently — every task gone, get() answering null as if the id had
    // never existed.
    const mem = CairnQ.sqlite(":memory:");
    const t = await mem.submit("a", {});
    expect(await mem.get(t.id)).not.toBeNull();
    await mem.close();
    await expect(mem.submit("b", {})).rejects.toThrow(StoreClosed);
    // And it stays refused: this is not the barrier a close in progress raises,
    // which clears when that close finishes.
    await expect(mem.submit("c", {})).rejects.toThrow(StoreClosed);
  });
});
