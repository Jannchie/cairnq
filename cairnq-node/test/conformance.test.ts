import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { CairnQ, STATUSES } from "../src/index.js";
import { TERMINAL } from "../src/models.js";
import { findProtocolRoot, loadMigrations } from "../src/sql.js";
import { Runner } from "./runner.js";

const scenarioDir = join(findProtocolRoot(), "conformance", "scenarios");
const files = readdirSync(scenarioDir)
  .filter((f) => f.endsWith(".json"))
  .sort();

describe("conformance", () => {
  it("has scenarios", () => {
    expect(files.length).toBeGreaterThan(0);
  });

  it("status set matches the protocol migration CHECK", () => {
    // Pin the SDK status set against the canonical CHECK constraint in the
    // migration (the cross-language source of truth). Both SDKs run this, so a
    // change to the SQL or to either SDK without the other fails here.
    const sql = loadMigrations()
      .map((m) => m.sql)
      .join("\n");
    const match = sql.match(/status\s+in\s*\(([^)]*)\)/i);
    expect(match).not.toBeNull();
    const sqlStatuses = new Set([...match![1].matchAll(/'([^']+)'/g)].map((x) => x[1]));
    expect(sqlStatuses).toEqual(new Set(STATUSES));
    expect(TERMINAL.every((s) => (STATUSES as readonly string[]).includes(s))).toBe(true);
  });

  for (const file of files) {
    it(file.replace(".json", ""), async () => {
      const data = JSON.parse(readFileSync(join(scenarioDir, file), "utf-8"));
      const dir = mkdtempSync(join(tmpdir(), "cairnq-"));
      const client = CairnQ.sqlite(join(dir, "t.db"));
      await client.connect();
      try {
        await new Runner(client).run(data.steps);
      } finally {
        await client.close();
      }
    });
  }
});
