import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { JSON_COLUMNS, MS_COLUMNS, rowToTask } from "../src/models.js";
import { findProtocolRoot } from "../src/sql.js";

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

/** Column names of one type in the canonical cairnq_tasks DDL. */
function columnsOfType(type: string): string[] {
  const ddl = readFileSync(
    join(findProtocolRoot(), "migrations", "postgres", "0001_init.sql"),
    "utf-8",
  );
  // Scoped to cairnq_tasks: cairnq_task_keys has *_ms columns of its own, and
  // they are not part of a Task row.
  const table = /create table if not exists cairnq_tasks \(([\s\S]*?)\n\);/.exec(ddl);
  if (!table) throw new Error("cannot find the cairnq_tasks DDL");
  return [...table[1].matchAll(new RegExp(`^\\s*(\\w+)\\s+${type}\\b`, "gm"))].map(
    (m) => m[1],
  );
}

// Which columns need normalizing is a fact about the SCHEMA, but rowToTask keeps
// it as a hand-written list — and so does the Python SDK, separately. Adding a
// bigint column to the migration and forgetting one of those lists is a silent
// failure: TypeScript erases the `number` type, so the field just arrives as a
// string, and the conformance suite compares shared behavior, so it stays green
// whether one SDK misses it or both do. The lists cost nothing at runtime; this
// is what keeps them honest.
describe("the normalized column lists match the canonical DDL", () => {
  it("covers every bigint column of cairnq_tasks", () => {
    expect([...MS_COLUMNS].sort()).toEqual(columnsOfType("bigint").sort());
  });

  it("covers every jsonb column of cairnq_tasks", () => {
    expect([...JSON_COLUMNS].sort()).toEqual(columnsOfType("jsonb").sort());
  });
});
