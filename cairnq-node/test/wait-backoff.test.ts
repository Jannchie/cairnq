// wait/call polling. PROTOCOL.md describes a 100–500ms backoff; the implementation
// polled at a flat 150ms, so waiting on a long task issued ~7 reads a second for
// its whole duration.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ } from "../src/index.js";
import { nextPollMs } from "../src/wait.js";
import { freshDbPath, sleep, succeedNext } from "./helpers.js";

let client: CairnQ;

beforeEach(async () => {
  client = CairnQ.sqlite(freshDbPath());
  await client.connect();
});

afterEach(async () => {
  await client.close();
});

describe("wait backoff", () => {
  it("backs off towards the ceiling", () => {
    expect(nextPollMs(100, 500)).toBe(150);
    expect(nextPollMs(150, 500)).toBe(225);
    expect(nextPollMs(400, 500)).toBe(500);
    expect(nextPollMs(500, 500)).toBe(500);
    // Truncation must not pin tiny intervals: Math.floor(1 * 1.5) === 1 would
    // otherwise re-read the task as fast as possible for the whole timeout.
    expect(nextPollMs(1, 500)).toBeGreaterThan(1);
    expect(nextPollMs(0, 500)).toBeGreaterThan(0);
  });

  it("does not re-read every 150ms while waiting on a slow task", async () => {
    const task = await client.submit("never", {});
    const store = client.store;
    const realProbe = store.getStatus.bind(store);
    let reads = 0;
    store.getStatus = async (id: string) => {
      reads += 1;
      return realProbe(id);
    };
    try {
      await client.wait(task.id, { timeoutMs: 1_200 });
    } catch {
      // expected: it never finishes
    } finally {
      store.getStatus = realProbe;
    }
    // Flat 150ms would be ~8 reads; 100ms backing off by 1.5x is 6 — plus one
    // more when the final remaining-capped nap's timer fires a millisecond
    // early (Node truncates timeouts to whole ms), landing a beat just before
    // the deadline.
    expect(reads).toBeLessThanOrEqual(7);
  });

  it("reads the full row only on the terminal beat, not per poll", async () => {
    // The poll loop's repeated read is the status-only probe; a 395KB payload
    // must not be re-read and re-parsed on every beat of a five-minute queue.
    const task = await client.submit("job", {});
    const store = client.store;
    const realGet = store.get.bind(store);
    let fullReads = 0;
    store.get = async (id: string) => {
      fullReads += 1;
      return realGet(id);
    };
    try {
      const waiting = client.wait(task.id, { timeoutMs: 3_000, pollMs: 20 });
      await sleep(150);
      await succeedNext(client, { ok: true });
      const done = await waiting;
      expect(done.status).toBe("succeeded");
      expect(done.result).toEqual({ ok: true });
    } finally {
      store.get = realGet;
    }
    expect(fullReads).toBe(1);
  });

  it("honors maxPollMs as the backoff ceiling", async () => {
    // §7: call/wait must pass the ceiling through — with it stuck at 500ms, a
    // known-slow task cannot trade detection latency for fewer reads. The naps
    // the loop *asks for* are the assertion; the stub sleeps briefly and ends
    // the task once the ask crosses the default ceiling (or gives up), so the
    // test never waits the timeout out.
    const task = await client.submit("slow", {});
    const store = client.store;
    const realWake = store.taskDoneWake.bind(store);
    const naps: number[] = [];
    store.taskDoneWake = async (id: string, ms: number) => {
      naps.push(ms);
      if (ms > 500 || naps.length > 30) await succeedNext(client);
      else await realWake(id, 10);
    };
    try {
      const done = await client.wait(task.id, {
        timeoutMs: 60_000,
        pollMs: 100,
        maxPollMs: 2_000,
      });
      expect(done.status).toBe("succeeded");
    } finally {
      store.taskDoneWake = realWake;
    }
    // Growing 1.5x from 100ms crosses the default 500ms ceiling only if the
    // caller's ceiling actually reached the loop.
    expect(Math.max(...naps)).toBeGreaterThan(500);
    expect(Math.max(...naps)).toBeLessThanOrEqual(2_000);
  });
});
