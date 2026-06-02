import { describe, expect, it } from "vitest";

import { loadStatements } from "../src/sql.js";
import { toPositional } from "../src/store/postgres.js";

// PostgresStore behavior (claim races, lock semantics) is exercised by the
// conformance suite against a real PG instance. These are pure-function tests for
// the named->positional translator — the one piece of PG plumbing not in SQL.
describe("toPositional", () => {
  it("collapses a repeated name to ONE positional slot", () => {
    const { text, values } = toPositional(
      "select * from t where (:status is null or status = :status) and id = :id",
      { status: "queued", id: "x" },
    );
    expect(text).toBe("select * from t where ($1 is null or status = $1) and id = $2");
    expect(values).toEqual(["queued", "x"]);
  });

  it("strips comments so a :name inside one is never a parameter", () => {
    const { text, values } = toPositional(
      "-- extend lease (now + :lease_ms)\nupdate t set x = :x where id = :id",
      { x: 1, id: "a" },
    );
    expect(text).not.toContain("lease_ms");
    expect(values).toEqual([1, "a"]);
  });

  it("leaves :: casts intact while still binding the named param", () => {
    const { text } = toPositional("where queue = any(:queues::text[])", { queues: ["default"] });
    expect(text).toBe("where queue = any($1::text[])");
  });

  it("translates every real postgres statement with no leftover :named params", () => {
    for (const [name, sql] of Object.entries(loadStatements("postgres"))) {
      const { text } = toPositional(sql, {});
      expect(text, name).not.toMatch(/(?<!:):\w/);
    }
  });
});
