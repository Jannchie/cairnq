import { describe, expect, it } from "vitest";

import { loadStatements } from "../src/sql.js";
import { positionalStatement, toPositional } from "../src/store/postgres.js";

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

  it("does the rewrite once per statement", () => {
    // Statement text is loaded once and never varies, so the rewrite is done
    // once. Without this every Postgres query re-scans its SQL with two regexes
    // — on the worker's poll loop, for a result that cannot have changed.
    const sql = loadStatements("postgres").claim;
    expect(positionalStatement(sql)).toBe(positionalStatement(sql));
  });

  it("still binds per-call values behind the cache", () => {
    const sql = "select * from t where id = :id";
    expect(toPositional(sql, { id: "a" }).values).toEqual(["a"]);
    expect(toPositional(sql, { id: "b" }).values).toEqual(["b"]);
  });
});
