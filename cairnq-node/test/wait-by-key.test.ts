// Picking a wait back up after it timed out.
//
// `waitTimeoutMs` bounds the wait, not the task — the task runs on. Getting at
// that result afterwards used to mean submitting again under the key and hoping
// the conflict strategy handed the finished task back, which is a re-submit
// dressed as a read. `wait(err.taskId)` and `waitByKey(key)` make it a read.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, TaskTimeout } from "../src/index.js";
import { freshDbPath, sleep } from "./helpers.js";

let client: CairnQ;

beforeEach(async () => {
  client = CairnQ.sqlite(freshDbPath());
  await client.connect();
});

afterEach(async () => {
  await client.close();
});

/** Finish a claimed task out of band, the way a worker elsewhere would. */
async function finishNext(result: unknown): Promise<void> {
  const [task] = await client.store.claim({
    queues: ["default"],
    workerId: "w1",
    leaseMs: 5_000,
  });
  await client.store.succeed({ taskId: task.id, workerId: "w1", result });
}

describe("waiting again after a timeout", () => {
  it("re-attaches by id, without re-running the task", async () => {
    const err = await client
      .call("job", {}, { key: "A", waitTimeoutMs: 50 })
      .then(() => null)
      .catch((e) => e);
    expect(err).toBeInstanceOf(TaskTimeout);
    expect(err.taskId).toBeTruthy();
    expect(err.key).toBeNull();

    await finishNext({ n: 1 });
    const done = await client.wait(err.taskId, { timeoutMs: 2_000 });
    expect(done.status).toBe("succeeded");
    expect(done.result).toEqual({ n: 1 });
    // One task, one run: the second wait read the store, it did not submit.
    expect((await client.list({})).length).toBe(1);
  });

  it("re-attaches by key, for a process that never held the id", async () => {
    await client.submit("job", {}, { key: "A" });
    const waiting = client.waitByKey("A", { timeoutMs: 2_000 });
    await finishNext({ n: 2 });
    expect((await waiting).result).toEqual({ n: 2 });
  });

  it("follows the key onto a replacement task", async () => {
    // A key points at whichever task is current, so a `replace` landing mid-wait
    // moves the wait rather than reporting the old task's cancellation.
    const first = await client.submit("job", {}, { key: "A" });
    const waiting = client.waitByKey("A", { timeoutMs: 3_000 });
    await sleep(30);
    const second = await client.submit("job", {}, { key: "A", conflict: "replace" });
    expect((await client.get(first.id))?.status).toBe("canceled");

    await finishNext({ n: 3 });
    const done = await waiting;
    expect(done.id).toBe(second.id);
    expect(done.result).toEqual({ n: 3 });
  });

  it("waits for a key that points at nothing yet", async () => {
    const waiting = client.waitByKey("A", { timeoutMs: 3_000 });
    await sleep(30);
    await client.submit("job", {}, { key: "A" });
    await finishNext({ n: 4 });
    expect((await waiting).result).toEqual({ n: 4 });
  });

  it("says which key it was watching when it gives up", async () => {
    const err = await client
      .waitByKey("missing", { timeoutMs: 30 })
      .then(() => null)
      .catch((e) => e);
    expect(err).toBeInstanceOf(TaskTimeout);
    expect(err.key).toBe("missing");
    expect(err.message).toMatch(/key missing/);
    expect(err.message).toMatch(/no task under this key/);
  });
});
