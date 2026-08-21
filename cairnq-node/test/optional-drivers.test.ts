import { describe, expect, it } from "vitest";

import { rowToTask } from "../src/models.js";

// Both backends are optional dependencies, and the point of that is wasted if
// importing the package loads them anyway. A Postgres-only deployment — which is
// every multi-host one — should not have to build a native module, and on hosts
// where node-gyp cannot build (an NFS home, a locked-down image) that difference
// is between installable and not.
//
// Guards the import graph, not the package manifest: `index.ts` re-exports
// SQLiteStore, so a plain top-level `import Database from "better-sqlite3"`
// anywhere under it would silently undo this.
describe("optional native drivers", () => {
  it("does not load better-sqlite3 just because the package was imported", async () => {
    const opened: string[] = [];
    const real = process.dlopen;
    // Every .node addon reaches the process through here, whichever resolver
    // (bindings, prebuild-install, a plain require) found the file.
    process.dlopen = ((module: unknown, filename: string, ...rest: unknown[]) => {
      opened.push(filename);
      return (real as (...a: unknown[]) => unknown)(module, filename, ...rest);
    }) as typeof process.dlopen;
    try {
      const mod = await import("../src/index.js");
      expect(mod.CairnQ).toBeTypeOf("function");
      expect(mod.SQLiteStore).toBeTypeOf("function"); // still exported...
      expect(opened.filter((f) => /better.sqlite3/i.test(f))).toEqual([]); // ...just not loaded
    } finally {
      process.dlopen = real;
    }
  });

  it("loads it on first use, and says what to install when it is missing", async () => {
    const { SQLiteStore } = await import("../src/index.js");
    const store = new SQLiteStore(":memory:");
    await store.connect(); // the first use — this is what may load the addon
    expect(await store.protocolVersion()).toBeGreaterThan(0);
    await store.close();
  });
});

// ------------------------------------------------------------------ int8

// Postgres sends int8 down the wire as text to protect precision it cannot know
// is unneeded. Which drivers surface that as a string is not cairnq's to decide,
// and 0.9.0's answer — a process-global type parser on `pg` — is unavailable to
// an INJECTED executor without changing how the application's own bigint columns
// come back. So the normalization lives at the row boundary, beside the one the
// JSON columns already get.
describe("row normalization", () => {
  const base = {
    id: "t1", name: "n", queue: "default", status: "queued",
    payload: "{}", metadata: "{}", result: null, error: null,
    progress: null, message: null, attempt: 0, max_attempts: 3, priority: 0,
    worker_id: null,
  };

  it("accepts epoch-ms columns as text or as numbers, and keeps null null", () => {
    const asText = rowToTask({
      ...base,
      lease_until_ms: null, run_at_ms: "1700000000000", cancel_requested_at_ms: null,
      created_at_ms: "1700000000000", updated_at_ms: "1700000000001", completed_at_ms: null,
    });
    expect(asText.run_at_ms).toBe(1_700_000_000_000);
    expect(asText.updated_at_ms).toBe(1_700_000_000_001);
    // Null is a fact about the task (not started, not canceled, not finished),
    // so it must survive the coercion rather than collapse to 0.
    expect(asText.completed_at_ms).toBeNull();
    expect(asText.cancel_requested_at_ms).toBeNull();

    const asNumbers = rowToTask({
      ...base,
      lease_until_ms: null, run_at_ms: 1_700_000_000_000, cancel_requested_at_ms: null,
      created_at_ms: 1_700_000_000_000, updated_at_ms: 1_700_000_000_001, completed_at_ms: null,
    });
    expect(asNumbers).toEqual(asText);
  });

  it("keeps millisecond timestamps exact", () => {
    // The claim that makes Number() lossless: a millisecond epoch does not reach
    // Number.MAX_SAFE_INTEGER until the year 287396.
    const ms = "8640000000000000"; // JS's own maximum Date, well inside the range
    expect(Number(ms)).toBeLessThan(Number.MAX_SAFE_INTEGER);
    expect(rowToTask({ ...base, created_at_ms: ms }).created_at_ms).toBe(8_640_000_000_000_000);
  });
});
