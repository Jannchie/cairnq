// Where the SDK loads the protocol from. The failure this pins: a vendored
// `_protocol/` (written by scripts/vendor-protocol.mjs at publish time) used to
// win over the repo's cairnq-protocol/, so edits to the canonical SQL were
// silently not under test. The Python SDK has the mirror of this test; the fix
// was made in both, so the regression coverage belongs in both.
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import { findProtocolRoot } from "../src/sql.js";

const repoProtocol = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "cairnq-protocol");

describe("protocol resolution", () => {
  it("prefers the repo's protocol over a vendored copy in a dev checkout", () => {
    if (!existsSync(join(repoProtocol, "sql"))) return; // installed package, not a checkout
    expect(findProtocolRoot()).toBe(repoProtocol);
  });

  it("resolves to a root that carries the conformance scenarios", () => {
    expect(existsSync(join(findProtocolRoot(), "conformance", "scenarios"))).toBe(true);
  });
});
