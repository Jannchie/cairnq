import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Locate the shared cairnq-protocol dir. Resolution: $CAIRNQ_PROTOCOL_DIR ->
// vendored `_protocol/` next to this module -> walk up to `cairnq-protocol/`
// (monorepo dev). Both SDKs load the SAME .sql strings (zero-drift guarantee).
// The dir is laid out per-dialect (sql/<dialect>/*.sql, migrations/<dialect>/*.sql)
// so a second backend (Postgres) slots in beside sqlite; `dialect` picks the subtree.
export function findProtocolRoot(): string {
  const env = process.env.CAIRNQ_PROTOCOL_DIR;
  if (env) return env;
  let dir = dirname(fileURLToPath(import.meta.url));
  const vendored = join(dir, "_protocol");
  if (existsSync(join(vendored, "sql"))) return vendored;
  for (let i = 0; i < 10; i++) {
    const candidate = join(dir, "cairnq-protocol");
    if (existsSync(join(candidate, "sql"))) return candidate;
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error("cannot locate cairnq-protocol; set CAIRNQ_PROTOCOL_DIR");
}

export function loadStatements(
  dialect = "sqlite",
  root = findProtocolRoot(),
): Record<string, string> {
  const dir = join(root, "sql", dialect);
  const out: Record<string, string> = {};
  for (const file of readdirSync(dir).sort()) {
    if (file.endsWith(".sql")) {
      out[file.slice(0, -4)] = readFileSync(join(dir, file), "utf-8");
    }
  }
  return out;
}

export function loadMigrations(
  dialect = "sqlite",
  root = findProtocolRoot(),
): { name: string; sql: string }[] {
  const dir = join(root, "migrations", dialect);
  return readdirSync(dir)
    .filter((f) => f.endsWith(".sql"))
    .sort()
    .map((f) => ({ name: f, sql: readFileSync(join(dir, f), "utf-8") }));
}
