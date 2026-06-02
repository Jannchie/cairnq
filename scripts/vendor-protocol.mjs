// Vendor the shared cairnq-protocol (sql + migrations) into each SDK package so
// published artifacts are self-contained. Run before `uv build` / `pnpm pack`.
// In the monorepo, SDKs resolve the protocol by walking up, so this is only
// needed for publishing. Both SDKs' loaders prefer a local `_protocol/` dir.
import { cpSync, mkdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const src = join(root, "cairnq-protocol");
const targets = [
  join(root, "cairnq-py", "cairnq", "_protocol"),
  join(root, "cairnq-node", "src", "_protocol"),
];

for (const target of targets) {
  rmSync(target, { recursive: true, force: true });
  mkdirSync(target, { recursive: true });
  cpSync(join(src, "sql"), join(target, "sql"), { recursive: true });
  cpSync(join(src, "migrations"), join(target, "migrations"), { recursive: true });
  console.log("vendored protocol ->", target);
}
