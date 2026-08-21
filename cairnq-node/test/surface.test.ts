import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { CairnQ, TaskContext, Worker } from "../src/index.js";
import { findProtocolRoot } from "../src/sql.js";

// The parity gate. See cairnq-protocol/surface.json for why it exists: the
// conformance suite compares SHARED behavior, so a capability only one SDK has
// nothing to disagree with, and drift between the two is invisible to it.
//
// This asserts both directions against the shared declaration. The second —
// every member here is declared there — is the load-bearing one: it makes a
// one-sided addition fail on the side that added it, while it is being added.

interface Exemption {
  member: string;
  reason: string;
}

interface Declared {
  shared: string[];
  internal: string[];
  only_node: Exemption[];
  only_py: Exemption[];
}

const surface: { classes: Record<string, Declared> } = JSON.parse(
  readFileSync(join(findProtocolRoot(), "surface.json"), "utf-8"),
);

const CLASSES: Record<string, new (...args: never[]) => unknown> = {
  CairnQ: CairnQ as never,
  Worker: Worker as never,
  TaskContext: TaskContext as never,
};

/** snake_case, the declaration's canonical form. */
function canonical(name: string): string {
  return name.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
}

/**
 * Every member this class exposes at runtime, canonicalized.
 *
 * Prototype members plus static ones (the sqlite/postgres factories). Public
 * instance FIELDS would be invisible here, which is why the classes expose their
 * few via getters — a member that cannot be seen cannot be gated.
 */
function membersOf(cls: new (...args: never[]) => unknown): string[] {
  const own = Object.getOwnPropertyNames(cls.prototype).filter((n) => n !== "constructor");
  const statics = Object.getOwnPropertyNames(cls).filter(
    (n) => !["length", "name", "prototype"].includes(n),
  );
  return [...new Set([...own, ...statics])].map(canonical).sort();
}

function names(entries: Exemption[]): string[] {
  return entries.map((e) => e.member);
}

describe.each(Object.keys(CLASSES))("%s matches the declared surface", (cls) => {
  const declared = surface.classes[cls];
  const actual = new Set(membersOf(CLASSES[cls]));

  it("has every member both SDKs are supposed to share", () => {
    const missing = declared.shared.filter((m) => !actual.has(m));
    expect(missing, `declared in surface.json but absent from cairnq-node`).toEqual([]);
  });

  it("has the members claimed as Node-only, and none claimed as Python-only", () => {
    expect(names(declared.only_node).filter((m) => !actual.has(m))).toEqual([]);
    // A member listed as Python-only that turns up here means the asymmetry was
    // closed and the exemption outlived its reason.
    expect(
      names(declared.only_py).filter((m) => actual.has(m)),
      "exists in cairnq-node but surface.json still calls it Python-only",
    ).toEqual([]);
  });

  it("declares every member it exposes", () => {
    const known = new Set([
      ...declared.shared,
      ...declared.internal,
      ...names(declared.only_node),
    ]);
    const undeclared = [...actual].filter((m) => !known.has(m));
    expect(
      undeclared,
      "add these to cairnq-protocol/surface.json — to `shared` (and implement " +
        "them in cairnq-py), to `only_node` with a reason, or to `internal`",
    ).toEqual([]);
  });
});

describe("the declaration itself", () => {
  it("covers every class the gate knows about", () => {
    expect(Object.keys(surface.classes).sort()).toEqual(Object.keys(CLASSES).sort());
  });

  it("gives a reason for every deliberate asymmetry", () => {
    for (const [cls, declared] of Object.entries(surface.classes)) {
      for (const side of ["only_node", "only_py"] as const) {
        for (const entry of declared[side]) {
          // A bare name would make the list a place to park gaps. The reason is
          // what turns it into a claim someone can disagree with in review.
          expect(typeof entry, `${cls}.${side}: entries need a reason`).toBe("object");
          expect(entry.reason.length).toBeGreaterThan(20);
        }
      }
    }
  });
});
