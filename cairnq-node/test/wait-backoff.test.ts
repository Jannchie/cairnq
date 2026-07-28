// wait/call polling. PROTOCOL.md describes a 100–500ms backoff; the implementation
// polled at a flat 150ms, so waiting on a long task issued ~7 reads a second for
// its whole duration.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ } from "../src/index.js";
import { nextPollMs } from "../src/wait.js";
import { freshDbPath } from "./helpers.js";

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
  });

  it("does not re-read every 150ms while waiting on a slow task", async () => {
    const task = await client.submit("never", {});
    const store = client.store;
    const realGet = store.get.bind(store);
    let reads = 0;
    store.get = async (id: string) => {
      reads += 1;
      return realGet(id);
    };
    try {
      await client.wait(task.id, { timeoutMs: 1_200 });
    } catch {
      // expected: it never finishes
    } finally {
      store.get = realGet;
    }
    // Flat 150ms would be ~8 reads; 100ms backing off by 1.5x is ~6 at most.
    expect(reads).toBeLessThanOrEqual(6);
  });
});
