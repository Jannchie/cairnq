"""Locate and load the shared cairnq-protocol SQL + migrations.

Resolution order: $CAIRNQ_PROTOCOL_DIR -> the `cairnq-protocol/` dir found by
walking up from this file (monorepo dev) -> a vendored `_protocol/` next to this
package (written at publish time). Both SDKs load the SAME .sql strings — this is
the zero-drift guarantee. The dir is laid out per-dialect (sql/<dialect>/*.sql,
migrations/<dialect>/*.sql) so a second backend (Postgres) slots in beside sqlite;
the `dialect` argument picks the subtree.

The source tree wins over the vendored copy on purpose: vendoring is a publish
step that also runs locally, and a stale `_protocol/` shadowing the canonical SQL
means edits to cairnq-protocol/ are silently not under test. An installed package
has no repo above it, so it falls through to the vendored copy."""

from __future__ import annotations

import os
from pathlib import Path

_MAX_WALK_UP = 10


def find_protocol_root() -> Path:
    env = os.environ.get("CAIRNQ_PROTOCOL_DIR")
    if env:
        return Path(env)
    here = Path(__file__).resolve()
    for up in list(here.parents)[:_MAX_WALK_UP]:
        cand = up / "cairnq-protocol"
        if (cand / "sql").is_dir():
            return cand
    vendored = here.parent / "_protocol"
    if (vendored / "sql").is_dir():
        return vendored
    raise RuntimeError(
        "cannot locate cairnq-protocol; set CAIRNQ_PROTOCOL_DIR to its path"
    )


def load_statements(dialect: str = "sqlite", root: Path | None = None) -> dict[str, str]:
    root = root or find_protocol_root()
    sql_dir = root / "sql" / dialect
    return {p.stem: p.read_text(encoding="utf-8") for p in sorted(sql_dir.glob("*.sql"))}


def load_migrations(dialect: str = "sqlite", root: Path | None = None) -> list[tuple[str, str]]:
    """Return (name, sql) pairs in filename order, for tracked application."""
    root = root or find_protocol_root()
    mig_dir = root / "migrations" / dialect
    return [(p.name, p.read_text(encoding="utf-8")) for p in sorted(mig_dir.glob("*.sql"))]
