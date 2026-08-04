// claim.sql has three specializations, each swapping a list-valued predicate for
// an equality so the planner can read the right index in claim order:
// claim_one_queue (queue), claim_one_name (name), claim_one_queue_one_name (both).
// Four near-identical canonical statements are exactly the drift the protocol's
// load-the-same-strings rule exists to prevent, so pin the relationship: every
// variant's SQL body must match claim.sql line for line except on the predicates
// it specializes. Anything else — a new column, a changed CASE arm, a reordered
// ORDER BY — fails here rather than silently applying to one caller shape only.
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

/** Which predicates each variant replaces, and what it replaces them with. */
const VARIANTS: Record<string, { equality: string; listPattern: RegExp }[]> = {
  claim_one_queue: [{ equality: "and queue = :queue", listPattern: /:queues/ }],
  claim_one_name: [{ equality: "and name = :name", listPattern: /:names/ }],
  claim_one_queue_one_name: [
    { equality: "and queue = :queue", listPattern: /:queues/ },
    { equality: "and name = :name", listPattern: /:names/ },
  ],
};

describe("claim variants mirror claim", () => {
  for (const dialect of ["sqlite", "postgres"]) {
    for (const [variant, swaps] of Object.entries(VARIANTS)) {
      it(`${variant} differs from claim on exactly its predicates (${dialect})`, () => {
        const statements = loadStatements(dialect);
        const many = body(statements.claim);
        const one = body(statements[variant]);

        expect(one).toHaveLength(many.length);
        const differing = many
          .map((line, i) => (line === one[i] ? null : i))
          .filter((i): i is number => i !== null);

        expect(differing).toHaveLength(swaps.length);
        // Compare in file order: the differing lines line up with the swaps as
        // listed, so a variant that swapped the wrong predicate fails here.
        for (const [n, i] of differing.entries()) {
          expect(one[i].trim()).toBe(swaps[n].equality);
          expect(many[i]).toMatch(swaps[n].listPattern);
        }
      });
    }
  }
});
