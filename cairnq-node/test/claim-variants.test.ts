// claim_one_queue.sql is claim.sql with one predicate swapped, for the
// single-queue case (see that file for the plans that justify a second statement).
// Two near-identical canonical statements are exactly the drift the protocol's
// load-the-same-strings rule exists to prevent, so pin the relationship: the SQL
// bodies must differ on exactly ONE line, and that line must be the queue filter.
// Anything else — a new column, a changed CASE arm, a reordered ORDER BY — fails
// here rather than silently applying to one-queue callers only.
import { describe, expect, it } from "vitest";

import { loadStatements } from "../src/sql.js";

/** The statement without its leading comment block, which is meant to differ. */
function body(sql: string): string[] {
  return sql
    .split("\n")
    .filter((line) => !line.startsWith("--"))
    .join("\n")
    .trim()
    .split("\n");
}

describe("claim_one_queue mirrors claim", () => {
  for (const dialect of ["sqlite", "postgres"]) {
    it(`differs from claim on exactly the queue predicate (${dialect})`, () => {
      const statements = loadStatements(dialect);
      const many = body(statements.claim);
      const one = body(statements.claim_one_queue);

      expect(one).toHaveLength(many.length);
      const differing = many
        .map((line, i) => (line === one[i] ? null : i))
        .filter((i): i is number => i !== null);

      expect(differing).toHaveLength(1);
      // The one difference binds a scalar :queue where claim binds the list.
      expect(one[differing[0]].trim()).toBe("and queue = :queue");
      expect(many[differing[0]]).toMatch(/:queues/);
    });
  }
});
