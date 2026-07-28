"""PostgresStore — the Postgres dialect of the shared cairnq-protocol SQL.

Everything protocol-shaped lives in TaskStore; this file is only what Postgres
does differently: an asyncpg pool, `:name` -> `$n` translation, and time taken
from the DB clock (`now()`) instead of from the SDK, which is what makes this
backend multi-host — unlike SQLite it coordinates API and worker processes across
machines, with no shared clock to agree on. claim uses FOR UPDATE SKIP LOCKED and
needs no claimable_probe, because PG readers don't block writers.

asyncpg is an optional dependency (install ``cairnq[postgres]``); it is imported
lazily in __init__ so the rest of the SDK works without it."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from functools import lru_cache
from typing import Any

from .._sql import load_migrations, load_statements
from ..errors import ProtocolVersionMismatch
from .base import COMMENT, NAMED, Fetch, TaskStore, statement_params

SUPPORTED_PROTOCOL_MAJOR = 1


@lru_cache(maxsize=None)
def positional_statement(sql: str) -> tuple[str, tuple[str, ...]]:
    """The rewritten SQL and the order its `$n` slots must be filled in.

    Translates the protocol's named-parameter SQL (`:name`) into asyncpg
    positional placeholders (`$1`), collapsing each DISTINCT name to ONE slot —
    statements reuse a name across CASE branches / IS NULL guards (e.g. list.sql).
    Which names count as parameters is `statement_params`' decision, shared with
    the SQLite binding path so the two can't disagree about, say, a `::type` cast.

    Memoized on the statement text, which is loaded once and never varies: this
    runs on every query, including the worker's poll loop, for a result that
    cannot have changed.
    """
    order = statement_params(sql)
    slot = {name: i + 1 for i, name in enumerate(order)}  # 1-based $n
    return NAMED.sub(lambda m: f"${slot[m.group(1)]}", COMMENT.sub("", sql)), order


def to_positional(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """The statement's rewritten text plus this call's values, in slot order.
    Names the statement does not use are simply not bound, so callers may pass a
    superset."""
    text, order = positional_statement(sql)
    return text, [params[n] for n in order]


class PostgresStore(TaskStore):
    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10):
        try:
            import asyncpg
        except ImportError as e:  # pragma: no cover - import guard
            raise RuntimeError(
                "PostgresStore requires asyncpg — install cairnq[postgres]"
            ) from e
        self._asyncpg = asyncpg
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool: Any = None
        self._init_lock = asyncio.Lock()
        self._sql = load_statements("postgres")

    # ------------------------------------------------------------------ setup
    async def connect(self) -> None:
        if self._pool is not None:
            return
        # The lock makes concurrent first-touch operations share one pool instead
        # of each racing to create its own (double-check after acquiring).
        async with self._init_lock:
            if self._pool is not None:
                return
            pool = await self._asyncpg.create_pool(
                self._dsn, min_size=self._min_size, max_size=self._max_size
            )
            try:
                async with pool.acquire() as conn:
                    await self._apply_migrations(conn)
                    version = await self._read_protocol_version(conn)
                    if version != SUPPORTED_PROTOCOL_MAJOR:
                        raise ProtocolVersionMismatch(
                            f"storage protocol_version={version}, "
                            f"SDK supports {SUPPORTED_PROTOCOL_MAJOR}"
                        )
            except BaseException:
                await pool.close()  # never leak a pool when connect fails
                raise
            self._pool = pool  # publish only a fully-migrated, version-checked pool

    async def _apply_migrations(self, conn: Any) -> None:
        await conn.execute(
            "create table if not exists cairnq_migrations "
            "(name text primary key, applied_at_ms bigint not null)"
        )
        for name, sql in load_migrations("postgres"):
            # Check and apply inside one transaction, with the row lock taken up
            # front: two processes cold-starting together would otherwise both see
            # a migration as unapplied and both run it.
            async with conn.transaction():
                await conn.execute("lock table cairnq_migrations in exclusive mode")
                already = await conn.fetchval(
                    "select 1 from cairnq_migrations where name = $1", name
                )
                if already is None:
                    await conn.execute(sql)  # multi-statement DDL
                    await conn.execute(
                        "insert into cairnq_migrations (name, applied_at_ms) values "
                        "($1, (extract(epoch from now()) * 1000)::bigint)",
                        name,
                    )

    async def close(self) -> None:
        if self._pool is not None:
            pool, self._pool = self._pool, None
            await pool.close()

    async def _read_protocol_version(self, conn: Any) -> int:
        row = await conn.fetchrow("select value from cairnq_meta where key = 'protocol_version'")
        return int(row["value"]) if row else 0

    async def protocol_version(self) -> int:
        await self._ensure()
        async with self._pool.acquire() as conn:
            return await self._read_protocol_version(conn)

    async def _ensure(self) -> None:
        if self._pool is None:
            await self.connect()

    # ----------------------------------------------------------- dialect seam
    async def _fetch(self, name: str, params: dict[str, Any]) -> list[Any]:
        await self._ensure()
        text, values = to_positional(self._sql[name], params)
        async with self._pool.acquire() as conn:
            return await conn.fetch(text, *values)

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncIterator[Fetch]:
        await self._ensure()
        async with self._pool.acquire() as conn, conn.transaction():

            async def fetch(name: str, params: dict[str, Any]) -> list[Any]:
                text, values = to_positional(self._sql[name], params)
                return await conn.fetch(text, *values)

            yield fetch
