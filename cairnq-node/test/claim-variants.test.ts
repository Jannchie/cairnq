// Several canonical statements ship specializations: claim swaps a list-valued
// predicate for an equality so the planner can read the right index in claim
// order (claim_one_queue, claim_one_name, claim_one_queue_one_name), and purge
// and stats swap an OPTIONAL filter for an equality because
// `(:p is null or col = :p)` is planned before the parameter has a value, so it
// reaches no index at all (purge_one_queue, purge_one_status,
// purge_one_queue_one_status, stats_one_queue).
//
// Near-identical canonical statements are exactly the drift the protocol's
// load-the-same-strings rule exists to prevent, so pin the relationship: every
// variant's SQL body must match its source line for line except on the
// predicates it specializes. Anything else — a new column, a changed CASE arm, a
// reordered ORDER BY — fails here rather than silently applying to one caller
// shape only.
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

/**
 * Every specialization, the statement it is derived from, and the predicates it
 * swaps. Two reasons produce these — claim trades a list-valued filter for an
 * equality so the planner can read the index in claim order, purge and stats
 * trade an OPTIONAL filter for an equality because `(:p is null or col = :p)` is
 * planned before the parameter has a value and reaches no index at all — but the
 * drift is identical, so one guard covers both.
 */
const VARIANTS: Record<
  string,
  { source: string; swaps: { equality: string; optionalPattern: RegExp }[] }
> = {
  claim_one_queue: {
    source: "claim",
    swaps: [{ equality: "and queue = :queue", optionalPattern: /:queues/ }],
  },
  claim_one_name: {
    source: "claim",
    swaps: [{ equality: "and name = :name", optionalPattern: /:names/ }],
  },
  claim_one_queue_one_name: {
    source: "claim",
    swaps: [
      { equality: "and queue = :queue", optionalPattern: /:queues/ },
      { equality: "and name = :name", optionalPattern: /:names/ },
    ],
  },
  purge_one_queue: {
    source: "purge",
    swaps: [{ equality: "and queue = :queue", optionalPattern: /:queue(::text)? is null/ }],
  },
  purge_one_status: {
    source: "purge",
    swaps: [{ equality: "and status = :status", optionalPattern: /:status(::text)? is null/ }],
  },
  purge_one_queue_one_status: {
    source: "purge",
    swaps: [
      { equality: "and queue = :queue", optionalPattern: /:queue(::text)? is null/ },
      { equality: "and status = :status", optionalPattern: /:status(::text)? is null/ },
    ],
  },
  stats_one_queue: {
    source: "stats",
    swaps: [{ equality: "where queue = :queue", optionalPattern: /:queue(::text)? is null/ }],
  },
};

describe("every variant mirrors the statement it specializes", () => {
  for (const dialect of ["sqlite", "postgres"]) {
    for (const [variant, { source, swaps }] of Object.entries(VARIANTS)) {
      it(`${variant} differs from ${source} on exactly its predicates (${dialect})`, () => {
        const statements = loadStatements(dialect);
        const general = body(statements[source]);
        const special = body(statements[variant]);

        expect(special).toHaveLength(general.length);
        const differing = general
          .map((line, i) => (line === special[i] ? null : i))
          .filter((i): i is number => i !== null);

        expect(differing).toHaveLength(swaps.length);
        // Compare in file order: the differing lines line up with the swaps as
        // listed, so a variant that swapped the wrong predicate fails here.
        for (const [n, i] of differing.entries()) {
          expect(special[i].trim()).toBe(swaps[n].equality);
          expect(general[i]).toMatch(swaps[n].optionalPattern);
        }
      });
    }
  }
});
