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

/** One reason, and the member or members it covers — see surface.json. */
interface Exemption {
  member?: string;
  members?: string[];
  reason: string;
}

interface Declared {
  shared: string[];
  internal?: string[];
  only_node: Exemption[];
  only_py: Exemption[];
}

const surface: {
  classes: Record<string, Declared>;
  modules: Omit<Declared, "internal">;
} = JSON.parse(readFileSync(join(findProtocolRoot(), "surface.json"), "utf-8"));

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
 * The same, for a module export rather than a class member. A name starting with
 * a capital is a type or a class, which both languages already spell the same
 * way; only the lowercase ones (functions, `defineTask`) are camelCase to
 * convert. Running those through `canonical` too would turn `CairnQ` into
 * `_cair_n_q`.
 */
function canonicalExport(name: string): string {
  return /^[A-Z]/.test(name) ? name : canonical(name);
}

/**
 * Everything `cairnq` exports, read from index.ts's SOURCE rather than by
 * importing it.
 *
 * `import * as` would see only the value exports: TypeScript erases the type
 * ones, and those are most of the surface worth gating (every `*Options`, every
 * handler signature, `PurgeInput`). Reading the barrel file is what makes the
 * types visible to a runtime test.
 */
function moduleExports(): string[] {
  const src = readFileSync(join(import.meta.dirname, "../src/index.ts"), "utf-8");
  const statement = /export\s+(?:type\s+)?\{([^}]*)\}\s*from\s*"[^"]*";/g;
  const found = [...src.matchAll(statement)].flatMap((m) =>
    m[1]
      .split(",")
      .map((n) => n.trim())
      .filter(Boolean)
      // `export { a as b }` publishes b.
      .map((n) => n.split(" as ").pop()!.trim()),
  );
  // A form this parser does not understand would silently shrink the surface
  // and make the gate pass by seeing less, so refuse to run rather than lie.
  const leftover = src.replace(statement, "");
  if (/\bexport\b/.test(leftover)) {
    throw new Error(
      `index.ts has an export form surface.test.ts cannot parse, so the module ` +
        `gate would silently skip it:\n${leftover.trim()}`,
    );
  }
  return [...new Set(found.map(canonicalExport))].sort();
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
  return entries.flatMap((e) => e.members ?? [e.member!]);
}

/**
 * Every surface the gate knows about: the three classes a caller drives, plus
 * what the package exports at all. They assert the same three directions, so
 * they run through the same block — the module surface is not a special case,
 * it is one more (declared, actual) pair. `internal` exists only for the
 * classes: TypeScript's `private` is erased at runtime.
 */
const SURFACES: Record<string, { declared: Declared; actual: Set<string> }> = {
  ...Object.fromEntries(
    Object.entries(CLASSES).map(([name, cls]) => [
      name,
      { declared: surface.classes[name], actual: new Set(membersOf(cls)) },
    ]),
  ),
  modules: { declared: surface.modules, actual: new Set(moduleExports()) },
};

describe.each(Object.keys(SURFACES))("%s matches the declared surface", (which) => {
  const { declared, actual } = SURFACES[which];

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
      ...(declared.internal ?? []),
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
    for (const [which, { declared }] of Object.entries(SURFACES)) {
      for (const side of ["only_node", "only_py"] as const) {
        for (const entry of declared[side]) {
          // A bare name would make the list a place to park gaps. The reason is
          // what turns it into a claim someone can disagree with in review.
          expect(typeof entry, `${which}.${side}: entries need a reason`).toBe("object");
          expect(entry.reason.length).toBeGreaterThan(20);
          expect(
            names([entry]).length,
            `${which}.${side}: an exemption needs a member or members`,
          ).toBeGreaterThan(0);
        }
      }
    }
  });
});
