// Regression tests for edge cases the library used to mishandle: non-JSON
// values crossing the protocol boundary, tasks stranded by unserializable
// results, bound handlers registering under "bound name", sub-second leases
// outrun by the heartbeat floor, and silent typo'd list filters. The Python
// twin is cairnq-py/tests/test_edge_cases.py.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, SerializationError, TaskError, TaskFailed, Worker } from "../src/index.js";
import { rowToTask } from "../src/models.js";
import { freshDbPath, sleep, taskRow } from "./helpers.js";

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

  // Both deterministic: the value cannot cross the JSON boundary however many
  // times the task is retried, so it must be recorded as a permanent failure on
  // the FIRST attempt rather than stranding the task `running` until lease
  // expiry. One case reaches that through JSON.stringify throwing (BigInt), the
  // other through the empties-itself-out rule (Map) — the settlement path is the
  // same, which is why they are one test.
  it.each([
    ["bigint", "big", { n: 1n }],
    ["opaque built-in", "mapper", new Map([["a", 1]])],
  ])("fails a task promptly when its result is unserializable (%s)", async (_label, name, value) => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 20 });
    let runs = 0;
    worker.task(name, async () => {
      runs += 1;
      return value;
    });
    await worker.background(async () => {
      const err = await rejection<TaskFailed>(
        client.call(name, {}, { timeoutMs: 3_000, pollMs: 20 }),
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
        client.call("bad-details", {}, { timeoutMs: 3_000, pollMs: 20 }),
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
      await expect(client.call("process", {}, { timeoutMs: 3_000, pollMs: 20 })).resolves.toEqual(
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
      await expect(client.call("slow", {}, { timeoutMs: 3_000, pollMs: 20 })).resolves.toEqual({
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

  it("rejects out-of-range numeric options", async () => {
    // maxAttempts=0 would still run once (claim increments before the check) —
    // a silently different meaning than the number says. The others silently
    // matched nothing or purged nothing.
    await expect(client.submit("job", {}, { maxAttempts: 0 })).rejects.toThrow("maxAttempts");
    await expect(client.submit("job", {}, { delayMs: -1 })).rejects.toThrow("delayMs");
    await expect(client.list({ limit: -1 })).rejects.toThrow("limit/offset");
    await expect(client.list({ offset: -1 })).rejects.toThrow("limit/offset");
    await expect(client.purge({ olderThanMs: -1 })).rejects.toThrow("olderThanMs");
    await expect(client.purge({ limit: 0 })).rejects.toThrow("limit");
  });

  it("rejects array elements JSON.stringify would mangle into null", async () => {
    // [undefined] / [fn] stringify as [null] — the twin SDK would read back a
    // null the caller never wrote. An undefined object property is merely
    // omitted (the JS idiom for "absent") and stays allowed.
    await expect(client.submit("job", { xs: [undefined] })).rejects.toThrow(SerializationError);
    await expect(client.submit("job", { xs: [() => 1] })).rejects.toThrow(SerializationError);
    const t = await client.submit("job", { xs: [null, 1], absent: undefined });
    expect(t.payload).toEqual({ xs: [null, 1] });
  });

  it("rejects the objects JSON.stringify would write as an empty one", async () => {
    // A Map/Set/typed array carries everything it holds in slots JSON.stringify
    // cannot see, so it serializes to `{}` (or, for a typed array, to an object
    // keyed by index) with the contents silently gone. Submitting one used to
    // succeed and store `{}`; a handler returning one used to record `{}` as the
    // task's result, succeeded and unrecoverable. The Python twin's encoder
    // already raises on the same class of value.
    for (const v of [
      new Map([["a", 1]]),
      new Set([1]),
      new Int32Array([1, 2]),
      new ArrayBuffer(4),
      /re/,
      new Error("boom"),
      // None of these were on the deny-list this rule replaced, and every one of
      // them used to be stored as `{}`: a platform type added after the list was
      // written (Float16Array is ES2025), two host objects, and a class whose
      // whole state is private — that last one is why the rule cannot be a list
      // of names at all, since its tag is plain `[object Object]`.
      ...(typeof Float16Array !== "undefined" ? [new Float16Array([1, 2])] : []),
      new URLSearchParams("a=b"),
      new Headers({ a: "b" }),
      new (class Private {
        #secret = 1;
        peek(): number {
          return this.#secret;
        }
      })(),
    ]) {
      await expect(client.submit("job", { v })).rejects.toThrow(SerializationError);
      // Nested and inside an array too — the check runs at every node.
      await expect(client.submit("job", { deep: { v } })).rejects.toThrow(SerializationError);
      await expect(client.submit("job", { xs: [v] })).rejects.toThrow(SerializationError);
    }
    // What still crosses: plain objects, class instances (their own enumerable
    // properties are exactly what gets written), Dates (toJSON, the JS idiom),
    // and empty objects — which trip the `{}` trace the strict pass keys off,
    // and must pass it.
    const kept = await client.submit("job", {
      plain: { a: 1 },
      inst: new (class Point {
        x = 1;
      })(),
      when: new Date(0),
      empty: {},
      indexed: { "0": "a literal zero key" },
    });
    expect(kept.payload).toEqual({
      plain: { a: 1 },
      inst: { x: 1 },
      when: "1970-01-01T00:00:00.000Z",
      empty: {},
      indexed: { "0": "a literal zero key" },
    });
  });

  it("does not re-parse a JSON column a decoding driver already decoded", async () => {
    // The Postgres case, reachable without a Postgres: `pg` decodes jsonb, so a
    // top-level JSON string arrives as a bare string — indistinguishable from
    // the TEXT wire form SQLite uses, and identical to what a caller stored.
    // rowToTask used to guess from the value and parse it twice: "s3://…" threw
    // a SyntaxError and "42" came back as the NUMBER 42. The store now tells it
    // which form it has (see TaskStore.jsonIsText / PostgresStore's probe).
    // taskRow's `jsonIsText` writes every JSON column in the matching form, so
    // the row and the flag it is read with cannot disagree.
    const decodedRow = (v: unknown) => taskRow({ payload: v, result: v }, false);
    const textRow = (v: unknown) =>
      taskRow({ payload: JSON.stringify(v), result: JSON.stringify(v) }, true);
    for (const value of ["hello", "42", "true", "null", "[1,2]", "", "s3://img/a.png"]) {
      // Decoded form: what `pg` hands back for a jsonb holding this string.
      const decoded = rowToTask(decodedRow(value), false);
      expect(decoded.payload).toBe(value);
      expect(decoded.result).toBe(value);
      // Text form: what SQLite (and asyncpg) hand back for the same value.
      expect(rowToTask(textRow(value), true).payload).toBe(value);
    }
    // Both forms agree on the non-string cases too, which is what made the bug
    // invisible: an object is an object either way.
    expect(rowToTask(decodedRow({ a: 1 }), false).payload).toEqual({ a: 1 });
    expect(rowToTask(textRow({ a: 1 }), true).payload).toEqual({ a: 1 });
    // A SQL NULL and a decoded JSON null both mean "no value" in either form.
    expect(rowToTask(decodedRow(null), false).result).toBeNull();
    expect(rowToTask(textRow(null), true).result).toBeNull();
  });

});
