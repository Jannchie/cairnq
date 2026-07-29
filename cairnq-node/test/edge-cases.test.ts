// Regression tests for edge cases the library used to mishandle: non-JSON
// values crossing the protocol boundary, tasks stranded by unserializable
// results, bound handlers registering under "bound name", sub-second leases
// outrun by the heartbeat floor, and silent typo'd list filters. The Python
// twin is cairnq-py/tests/test_edge_cases.py.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, SerializationError, TaskError, TaskFailed, Worker } from "../src/index.js";
import { freshDbPath, sleep } from "./helpers.js";

let dbPath: string;
let client: CairnQ;

/** Await a promise that must reject, handing back the rejection value. */
const rejection = async <T>(p: Promise<unknown>): Promise<T> =>
  p.then(() => {
    throw new Error("expected rejection");
  }, (e: unknown) => e as T);

beforeEach(async () => {
  dbPath = freshDbPath();
  client = CairnQ.sqlite(dbPath);
  await client.connect();
});

afterEach(async () => {
  await client.close();
});

describe("edge cases", () => {
  it("rejects non-finite numbers at submit", async () => {
    // JSON.stringify would silently turn NaN/Infinity into null — the stored
    // task then differs from what the caller meant, and the Python SDK rejects
    // the same values, so refuse loudly here too.
    await expect(client.submit("job", { x: NaN })).rejects.toThrow(SerializationError);
    await expect(client.submit("job", { x: 1 }, { metadata: { y: Infinity } })).rejects.toThrow(
      SerializationError,
    );
  });

  it("fails a task promptly when its result cannot be serialized", async () => {
    // A BigInt makes serialization throw inside complete(). Deterministic, so
    // it must be recorded as a permanent SerializationError on the first
    // attempt — not strand the task `running` until lease expiry.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20 });
    let runs = 0;
    worker.task("big", async () => {
      runs += 1;
      return { n: 1n };
    });
    await worker.background(async () => {
      const err = await rejection<TaskFailed>(
        client.call("big", {}, { waitTimeoutMs: 3_000, pollMs: 20 }),
      );
      expect(err).toBeInstanceOf(TaskFailed);
      expect(err.code).toBe("unserializable_result");
    });
    expect(runs).toBe(1);
  });

  it("records the failure even when TaskError details cannot be serialized", async () => {
    // The envelope is stripped to its string fields rather than stranding the
    // task until lease expiry.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20 });
    worker.task("bad-details", async () => {
      throw new TaskError("boom", { details: { weird: 1n } as never });
    });
    await worker.background(async () => {
      const err = await rejection<TaskFailed>(
        client.call("bad-details", {}, { waitTimeoutMs: 3_000, pollMs: 20 }),
      );
      expect(err).toBeInstanceOf(TaskFailed);
      expect(err.message).toBe("boom");
      expect(err.details).toEqual({});
    });
  });

  it("registers a bound method under the method's name", async () => {
    // fn.name of a bound function is "bound process"; the "bound " prefix is
    // stripped so the handler answers to the name a submit actually uses.
    const svc = {
      async process(_ctx: unknown, _payload: unknown) {
        return { ok: true };
      },
    };
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20 });
    worker.task(svc.process.bind(svc));
    await worker.background(async () => {
      await expect(client.call("process", {}, { waitTimeoutMs: 3_000, pollMs: 20 })).resolves.toEqual(
        { ok: true },
      );
    });
  });

  it("maintains a sub-second lease with the default heartbeat", async () => {
    // The heartbeat floor used to be 1s, so a lease below that could never be
    // maintained: the worker's own claim loop recovered the "expired" lease
    // and re-ran the task while the first attempt was still going.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20, leaseMs: 200, concurrency: 2 });
    let runs = 0;
    worker.task("slow", async () => {
      runs += 1;
      await sleep(400); // outlives two lease periods
      return { ok: true };
    });
    await worker.background(async () => {
      await expect(client.call("slow", {}, { waitTimeoutMs: 3_000, pollMs: 20 })).resolves.toEqual({
        ok: true,
      });
    });
    expect(runs).toBe(1);
  });

  it("rejects an unknown status filter in list", async () => {
    // A typo'd status used to match nothing and return [] indistinguishably
    // from "no such tasks".
    await expect(client.list({ status: "succeded" as never })).rejects.toThrow(
      "unknown status filter",
    );
  });
});
