// Keep cairnq-node (package.json) and cairnq-py (pyproject.toml) on ONE shared
// version — they publish together under a single `v*` tag, so they must match.
//
//   node scripts/bump-version.mjs                      print both versions + sync status
//   node scripts/bump-version.mjs 1.2.3                set both to an explicit version
//   node scripts/bump-version.mjs patch|minor|major    bump from the current shared version
//
// No-arg mode exits non-zero on drift, so it doubles as a CI sync guard. Edits
// are surgical (regex on the version field only) — surrounding formatting is left
// untouched, keeping diffs to a single line per file.
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const files = {
  "cairnq-node": { path: join(root, "cairnq-node", "package.json"), re: /("version"\s*:\s*")(\d[^"]*)(")/ },
  "cairnq-py": { path: join(root, "cairnq-py", "pyproject.toml"), re: /^(version\s*=\s*")(\d[^"]*)(")/m },
};

for (const f of Object.values(files)) {
  f.text = readFileSync(f.path, "utf8");
  const m = f.text.match(f.re);
  if (!m) throw new Error(`version field not found in ${f.path}`);
  f.version = m[2];
}
const [node, py] = [files["cairnq-node"], files["cairnq-py"]];
const arg = process.argv[2];

if (!arg) {
  const synced = node.version === py.version;
  console.log(`cairnq-node  ${node.version}`);
  console.log(`cairnq-py    ${py.version}`);
  console.log(synced ? "in sync ✓" : "OUT OF SYNC ✗");
  process.exit(synced ? 0 : 1);
}

let next;
if (["major", "minor", "patch"].includes(arg)) {
  if (node.version !== py.version) {
    console.error(`cannot bump: versions differ (node ${node.version} != py ${py.version}) — set an explicit version first`);
    process.exit(1);
  }
  const [a, b, c] = node.version.replace(/[-+].*$/, "").split(".").map(Number);
  next = arg === "major" ? `${a + 1}.0.0` : arg === "minor" ? `${a}.${b + 1}.0` : `${a}.${b}.${c + 1}`;
} else if (/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(arg)) {
  next = arg;
} else {
  console.error(`invalid version "${arg}" — expected MAJOR.MINOR.PATCH, or major|minor|patch`);
  process.exit(1);
}

for (const f of Object.values(files)) {
  writeFileSync(f.path, f.text.replace(f.re, `$1${next}$3`));
}
console.log(`${node.version} -> ${next}  (cairnq-node + cairnq-py)`);
