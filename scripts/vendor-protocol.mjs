// Vendor the shared cairnq-protocol (sql + migrations) into each SDK package so
// published artifacts are self-contained. In the monorepo, SDKs resolve the
// protocol by walking up, so this is only needed for publishing. Both SDKs'
// loaders look for a local `_protocol/` dir next to the module at runtime:
//   - cairnq-py runs from `cairnq/`         -> vendor into cairnq/_protocol
//   - cairnq-node runs from compiled `dist/` -> vendor into dist/_protocol
// So for node this must run AFTER `pnpm build` (tsc only emits .ts -> .js and
// never copies .sql); for py it can run any time before `uv build`.
import { cpSync, mkdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const src = join(root, "cairnq-protocol");
const targets = [
  join(root, "cairnq-py", "cairnq", "_protocol"),
  join(root, "cairnq-node", "dist", "_protocol"),
];

for (const target of targets) {
  rmSync(target, { recursive: true, force: true });
  mkdirSync(target, { recursive: true });
  cpSync(join(src, "sql"), join(target, "sql"), { recursive: true });
  cpSync(join(src, "migrations"), join(target, "migrations"), { recursive: true });
  console.log("vendored protocol ->", target);
}
