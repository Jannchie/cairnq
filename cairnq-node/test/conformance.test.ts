import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import pg from "pg";

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

// The scenarios are dialect-neutral by design, so the Postgres backend must pass
// the same suite — that is what keeps sql/postgres/*.sql from drifting from
// sql/sqlite/*.sql in behavior, not just in wording. Skipped without a DSN; CI's
// `postgres` job provides one.
const PG_DSN = process.env.CAIRNQ_TEST_PG_DSN;
const pgDescribe = PG_DSN ? describe : describe.skip;

pgDescribe("conformance (postgres)", () => {
  let client: CairnQ;
  let admin: pg.Pool;

  beforeAll(async () => {
    client = CairnQ.postgres(PG_DSN!);
    await client.connect(); // applies migrations
    admin = new pg.Pool({ connectionString: PG_DSN });
  });

  afterAll(async () => {
    await admin.end();
    await client.close();
  });

  // Scenarios assume an empty store; a real database is shared across them.
  beforeEach(() => admin.query("truncate cairnq_tasks, cairnq_task_keys").then(() => undefined));

  for (const file of files) {
    it(file.replace(".json", ""), async () => {
      const data = JSON.parse(readFileSync(join(scenarioDir, file), "utf-8"));
      await new Runner(client).run(data.steps);
    });
  }
});
