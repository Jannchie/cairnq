// What a key does when the task it points at has already finished.
//
// The conformance scenarios pin the single-caller branches (key_reuse_terminal,
// key_reuse_succeeded, key_reuse_failed). What they cannot express is the reason
// the branches live inside the keyed transaction at all: concurrent submits.
// Deciding "is this task still live?" outside it — read the status, then pick a
// strategy — puts an await between the read and the write, and two callers that
// both read the same finished task both replace, each cancelling the other's
// fresh task. That is the double-submit a key exists to prevent.
import { expect, it } from "vitest";

import type { CairnQ } from "../src/index.js";
import { describeBackends } from "./backends.js";

/** Run a task to `succeeded`, the way a worker would. */
async function finish(client: CairnQ, id: string, result: unknown = { n: 1 }): Promise<void> {
  await client.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 5_000 });
  await client.store.succeed({ taskId: id, workerId: "w1", result });
}

// Both dialects: the key lock this suite is really about is where they differ
// most — a no-op on SQLite, where BEGIN IMMEDIATE already serializes every keyed
// transaction, and a `pg_advisory_xact_lock` on Postgres, where READ COMMITTED
// gives two concurrent same-key submits nothing to serialize on. The concurrent
// case below is the one that would silently pass on SQLite while the Postgres
// lock was missing or wrong.
describeBackends("keyed submit against a finished task", (backend) => {
  it("frees the key after a failure instead of replaying it", async () => {
    const client = await backend.client();
    const first = await client.submit("job", {}, { key: "A", maxAttempts: 1 });
    await client.store.claim({ queues: ["default"], workerId: "w1", leaseMs: 5_000 });
    await client.store.fail({
      taskId: first.id,
      workerId: "w1",
      error: { type: "E", code: "boom", message: "boom", retryable: true },
      retryable: true,
      delayMs: 0,
    });

    const second = await client.submit("job", {}, { key: "A", maxAttempts: 1 });
    expect(second.id).not.toBe(first.id);
    expect(second.status).toBe("queued");
    // The failed task is left as it was: nothing re-cancels a settled row.
    expect((await client.get(first.id))?.status).toBe("failed");
  });

  it("hands back a succeeded result only under reuse-succeeded", async () => {
    const client = await backend.client();
    const first = await client.submit("job", {}, { key: "A" });
    await finish(client, first.id);

    const cached = await client.submit("job", {}, { key: "A", conflict: "reuse-succeeded" });
    expect(cached.id).toBe(first.id);
    expect(cached.result).toEqual({ n: 1 });

    // …and reuse-succeeded repointed nothing, so plain reuse still starts over.
    const rerun = await client.submit("job", {}, { key: "A" });
    expect(rerun.id).not.toBe(first.id);
    expect(rerun.status).toBe("queued");
  });

  it("collapses concurrent submits onto one task instead of cancelling each other", async () => {
    const client = await backend.client();
    const first = await client.submit("job", {}, { key: "A" });
    await finish(client, first.id);

    // The double-click, arriving twice at once against a key whose last task
    // finished. Whoever loses the race sees the winner's fresh task — queued,
    // so reusable — rather than starting a second one.
    const racers = await Promise.all(
      Array.from({ length: 8 }, () => client.submit("job", {}, { key: "A" })),
    );
    const ids = new Set(racers.map((t) => t.id));
    expect(ids.size).toBe(1);
    expect(ids.has(first.id)).toBe(false);

    const current = await client.getByKey("A");
    expect(current?.id).toBe(racers[0].id);
    expect(current?.status).toBe("queued");
    // No task was created and then thrown away: two rows exist, the finished
    // one and the new one.
    expect((await client.list({})).length).toBe(2);
  });

  it("still rejects on a finished task", async () => {
    const client = await backend.client();
    const first = await client.submit("job", {}, { key: "A", conflict: "reject" });
    await finish(client, first.id);
    // reject asks for a key that is used at most once, ever — a task reaching
    // a terminal state does not make the key free again.
    await expect(client.submit("job", {}, { key: "A", conflict: "reject" })).rejects.toThrow(
      /already exists/,
    );
  });

  it("replace still cancels a live task", async () => {
    const client = await backend.client();
    const first = await client.submit("job", {}, { key: "A" });
    const second = await client.submit("job", {}, { key: "A", conflict: "replace" });
    expect(second.id).not.toBe(first.id);
    expect((await client.get(first.id))?.status).toBe("canceled");
  });
});
