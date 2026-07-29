"""SQLiteStore — the SQLite dialect of the shared cairnq-protocol SQL.

Everything protocol-shaped lives in TaskStore; this file is only what SQLite does
differently: one aiosqlite connection guarded by an asyncio.Lock, BEGIN IMMEDIATE
transactions, a read-only probe in front of the write lock, and time supplied by
the SDK (`:now_ms`) rather than by the database.

Concurrency model: SQLite is a single writer anyway, and the lock prevents
multi-statement transactions (submit-with-key, recover+claim) from interleaving on
the shared connection. It is only ever held for short DB work — never while a task
handler runs. Cross-process contention is absorbed by busy_timeout."""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import aiosqlite

from .._ids import now_ms
from .._sql import load_migrations, load_statements
from .base import Fetch, TaskStore, check_protocol_version, statement_params


def _split_script(script: str) -> list[str]:
    """Split a migration into single statements.

    `executescript` would be the obvious tool, but it COMMITs before it runs —
    which would silently end the transaction the migration is supposed to be
    applied in. `sqlite3.complete_statement` gives the same split without the
    hidden commit, and understands strings and comments so a `;` inside either
    doesn't cut a statement in half."""
    statements: list[str] = []
    buffer = ""
    for line in script.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statements.append(buffer.strip())
            buffer = ""
    if buffer.strip():
        statements.append(buffer.strip())
    return statements


_WAL_RETRY_DELAY_S = 0.05
_WAL_RETRY_BUDGET_S = 5.0


def _is_memory(path: str) -> bool:
    """Whether this path names an in-memory database rather than a file."""
    return path == ":memory:" or "mode=memory" in path


async def _enable_wal(conn: aiosqlite.Connection) -> None:
    """Put the database in WAL mode, waiting out a concurrent cold start.

    journal_mode is a persistent property of the file, so only the first
    connection to a new database actually switches it — and that switch needs an
    exclusive lock. `busy_timeout` does not cover it: SQLite returns SQLITE_BUSY
    for a journal_mode change rather than invoking the busy handler, so several
    processes opening the same new database at once would otherwise get an
    instant "database is locked". Retry briefly instead; the window is only as
    long as one other opener's switch.

    Callers must skip in-memory databases: those report journal_mode = "memory"
    and can never be WAL, so waiting for one is waiting for something that will
    not happen.
    """
    deadline = asyncio.get_running_loop().time() + _WAL_RETRY_BUDGET_S
    while True:
        try:
            cur = await conn.execute("pragma journal_mode = WAL")
            row = await cur.fetchone()
            await cur.close()
            if row is not None and str(row[0]).lower() == "wal":
                return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc) and "busy" not in str(exc):
                raise
        if asyncio.get_running_loop().time() >= deadline:
            raise sqlite3.OperationalError(
                "could not switch the database to WAL mode: it stayed locked by "
                "another connection"
            )
        await asyncio.sleep(_WAL_RETRY_DELAY_S)


class SQLiteStore(TaskStore):
    def __init__(self, path: str, *, busy_timeout_ms: int = 5_000):
        self._path = path
        self._busy_timeout_ms = busy_timeout_ms
        self._conn: aiosqlite.Connection | None = None
        self._sql = load_statements("sqlite")
        self._lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()

    # ------------------------------------------------------------------ setup
    async def connect(self) -> None:
        if self._conn is not None:
            return
        async with self._init_lock:
            if self._conn is not None:
                return
            memory = _is_memory(self._path)
            if not memory:
                Path(self._path).parent.mkdir(parents=True, exist_ok=True)
            conn = await aiosqlite.connect(self._path, isolation_level=None)
            conn.row_factory = aiosqlite.Row
            # busy_timeout first, so every later statement waits out contention
            # instead of failing instantly.
            await conn.execute(f"pragma busy_timeout = {self._busy_timeout_ms}")
            # WAL exists so several processes can share one file. An in-memory
            # database is private to this connection, so there is nothing to
            # share and nothing to wait for.
            if not memory:
                await _enable_wal(conn)
            await conn.execute("pragma foreign_keys = ON")
            await self._apply_migrations(conn)
            self._conn = conn
            check_protocol_version(await self.protocol_version())

    async def _apply_migrations(self, conn: aiosqlite.Connection) -> None:
        await conn.execute(
            "create table if not exists cairnq_migrations "
            "(name text primary key, applied_at_ms integer not null)"
        )
        for name, sql in load_migrations("sqlite"):
            # Check and apply under one write lock. Two processes cold-starting on
            # a shared database would otherwise both see a migration as unapplied
            # and both run it — harmless for the idempotent ones, not for a future
            # ALTER. BEGIN IMMEDIATE serializes them; the loser sees it applied.
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur = await conn.execute(
                    "select 1 from cairnq_migrations where name = ?", (name,)
                )
                already = await cur.fetchone()
                await cur.close()
                if already is None:
                    for statement in _split_script(sql):
                        await conn.execute(statement)
                    await conn.execute(
                        "insert into cairnq_migrations (name, applied_at_ms) values (?, ?)",
                        (name, now_ms()),
                    )
            except BaseException:
                # Shielded and BaseException-suppressed: a cancellation landing
                # during the rollback must not abandon the shared connection
                # inside an open write transaction.
                with contextlib.suppress(BaseException):
                    await asyncio.shield(conn.execute("ROLLBACK"))
                raise
            else:
                await conn.execute("COMMIT")

    async def close(self) -> None:
        if self._conn is not None:
            conn, self._conn = self._conn, None
            await conn.close()

    async def _ensure(self) -> aiosqlite.Connection:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None
        return self._conn

    async def protocol_version(self) -> int:
        rows = await self._fetch("protocol_version", {})
        return int(rows[0]["value"]) if rows else 0

    # ----------------------------------------------------------- dialect seam
    def _bind(self, sql: str, params: dict[str, Any]) -> dict[str, Any]:
        """Adapt the dialect-neutral parameters to what this statement binds.

        SQLite statements carry no DB clock, so every absolute `*_ms` is derived
        here from one `now`, and booleans cross as 0/1. The result is narrowed to
        the names the SQL actually uses, which is what makes it safe for a caller
        to pass one superset of parameters for both dialects.

        Each derivation writes a name Postgres does not use (`lease_until_ms` from
        `lease_ms`, and so on), so a statement binds one or the other, never both.
        """
        now = now_ms()
        bound: dict[str, Any] = {}
        for name in statement_params(sql):
            if name == "now_ms":
                bound[name] = now
            elif name == "lease_until_ms":
                bound[name] = now + params["lease_ms"]
            elif name == "run_at_ms":
                bound[name] = now + params["delay_ms"]
            elif name == "before_ms":
                bound[name] = now - params["older_than_ms"]
            elif name == "queues":
                bound[name] = json.dumps(list(params["queues"]))
            elif name == "names":
                # json_each needs a JSON array; null stays null so the SQL's
                # `:names is null` arm means "no filter".
                value = params["names"]
                bound[name] = None if value is None else json.dumps(list(value))
            elif name in ("retryable", "reset_attempt"):
                bound[name] = 1 if params[name] else 0
            else:
                bound[name] = params[name]
        return bound

    async def _run(self, name: str, params: dict[str, Any]) -> list[aiosqlite.Row]:
        sql = self._sql[name]
        cur = await self._conn.execute(sql, self._bind(sql, params))
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def _fetch(self, name: str, params: dict[str, Any]) -> list[aiosqlite.Row]:
        await self._ensure()
        async with self._lock:
            return await self._run(name, params)

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncIterator[Fetch]:
        """BEGIN IMMEDIATE … COMMIT on the shared connection, rolling back on any
        error. The store lock is held for the whole transaction, so no other
        operation can slip a statement into it."""
        await self._ensure()
        async with self._lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._run
            except BaseException:
                # Shielded and BaseException-suppressed: a cancellation landing
                # during the rollback must not abandon the shared connection
                # inside an open write transaction — every later operation
                # would then run inside it, holding SQLite's write lock.
                with contextlib.suppress(BaseException):
                    await asyncio.shield(self._conn.execute("ROLLBACK"))
                raise
            else:
                await self._conn.execute("COMMIT")

    async def _has_claimable_work(self, params: dict[str, Any]) -> bool:
        # Read-only probe first: an idle worker never takes SQLite's single write
        # lock, so idle workers don't serialize against each other.
        rows = await self._fetch("claimable_probe", params)
        return bool(rows and rows[0]["has_work"])
