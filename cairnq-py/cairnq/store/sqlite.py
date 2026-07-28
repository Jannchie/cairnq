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
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import aiosqlite

from .._ids import now_ms
from .._sql import load_migrations, load_statements
from ..errors import ProtocolVersionMismatch
from .base import Fetch, TaskStore, statement_params

SUPPORTED_PROTOCOL_MAJOR = 1


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
            if self._path != ":memory:":
                Path(self._path).parent.mkdir(parents=True, exist_ok=True)
            conn = await aiosqlite.connect(self._path, isolation_level=None)
            conn.row_factory = aiosqlite.Row
            await conn.execute("pragma journal_mode = WAL")
            await conn.execute("pragma foreign_keys = ON")
            await conn.execute(f"pragma busy_timeout = {self._busy_timeout_ms}")
            await self._apply_migrations(conn)
            self._conn = conn
            await self._check_version()

    async def _apply_migrations(self, conn: aiosqlite.Connection) -> None:
        await conn.execute(
            "create table if not exists cairnq_migrations "
            "(name text primary key, applied_at_ms integer not null)"
        )
        cur = await conn.execute("select name from cairnq_migrations")
        applied = {row["name"] for row in await cur.fetchall()}
        await cur.close()
        for name, sql in load_migrations("sqlite"):
            if name in applied:
                continue
            await conn.executescript(sql)
            # `or ignore`: another process may apply the same migration concurrently
            # on a fresh shared db (mode B cold start). Migrations are idempotent.
            await conn.execute(
                "insert or ignore into cairnq_migrations (name, applied_at_ms) values (?, ?)",
                (name, now_ms()),
            )

    async def close(self) -> None:
        if self._conn is not None:
            conn, self._conn = self._conn, None
            await conn.close()

    async def _ensure(self) -> aiosqlite.Connection:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None
        return self._conn

    async def _check_version(self) -> None:
        version = await self.protocol_version()
        if version != SUPPORTED_PROTOCOL_MAJOR:
            raise ProtocolVersionMismatch(
                f"storage protocol_version={version}, SDK supports {SUPPORTED_PROTOCOL_MAJOR}"
            )

    async def protocol_version(self) -> int:
        conn = await self._ensure()
        cur = await conn.execute("select value from cairnq_meta where key = 'protocol_version'")
        row = await cur.fetchone()
        await cur.close()
        return int(row["value"]) if row else 0

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
                with contextlib.suppress(Exception):
                    await self._conn.execute("ROLLBACK")
                raise
            else:
                await self._conn.execute("COMMIT")

    async def _has_claimable_work(self, params: dict[str, Any]) -> bool:
        # Read-only probe first: an idle worker never takes SQLite's single write
        # lock, so idle workers don't serialize against each other.
        rows = await self._fetch("claimable_probe", params)
        return bool(rows and rows[0]["has_work"])
