"""The built-in executor: an asyncpg pool over a libpq DSN.

What ``CairnQ.postgres(dsn)`` uses, and the reference for what an adapter over
another driver must do. Mirrors ``createPoolExecutor`` in the TypeScript SDK.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any

from .pg_executor import ListenUnavailable, PgExecutor, PgSession

# A Postgres identifier that is safe to interpolate — cairnq quotes the schema
# name, and a name that could close that quote could rewrite the statement.
# Deliberately narrower than what Postgres accepts: a schema cairnq is asked to
# live in is a deployment decision, not a place to be clever. Mirrors
# PLAIN_IDENT in the TypeScript SDK's pg-pool.ts.
PLAIN_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def check_schema_name(schema: str) -> None:
    if not PLAIN_IDENT.match(schema):
        raise ValueError(
            "schema must be a plain identifier (letters, digits, _ and $, not "
            f"starting with a digit), got {schema!r}"
        )


class _ConnSession:
    """The PgSession face of one asyncpg connection or pool."""

    def __init__(self, conn: Any):
        self._conn = conn

    async def query(self, text: str, values: Sequence[Any]) -> list[Any]:
        # asyncpg's fetch already returns a list; copying it would be a full
        # shallow copy of every result set the SDK reads.
        return await self._conn.fetch(text, *values)

    async def execute(self, sql: str) -> None:
        # No arguments: asyncpg sends this over the simple query protocol, which
        # is what makes a multi-statement migration script legal here and not in
        # query().
        await self._conn.execute(sql)


class _PoolExecutor:
    def __init__(self, asyncpg: Any, pool: Any, dsn: str):
        self._asyncpg = asyncpg
        self._pool = pool
        self._dsn = dsn
        self._session = _ConnSession(pool)

    async def query(self, text: str, values: Sequence[Any]) -> list[Any]:
        return await self._session.query(text, values)

    async def execute(self, sql: str) -> None:
        await self._session.execute(sql)

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[PgSession]:
        async with self._pool.acquire() as conn, conn.transaction():
            yield _ConnSession(conn)

    async def listen(
        self,
        channels: Sequence[str],
        on_notify: Callable[[str, str | None], None],
        on_close: Callable[[], None],
    ) -> Callable[[], None]:
        # Built from the raw DSN rather than taken from the pool: a listener
        # holds its connection for its whole life, and a pooled one would be a
        # slot the store never gives back.
        conn = await self._asyncpg.connect(self._dsn)  # transient on failure

        def bridge(_c: Any, _pid: int, channel: str, payload: str | None) -> None:
            on_notify(channel, payload)

        try:
            for channel in channels:
                await conn.add_listener(channel, bridge)
            conn.add_termination_listener(lambda _c: on_close())
        except Exception as e:
            with contextlib.suppress(Exception):
                await conn.close()
            # Connected, but LISTEN was refused (e.g. a transaction-mode
            # pooler) — deterministic, so tell the store not to retry.
            raise ListenUnavailable(str(e)) from e

        def stop() -> None:
            with contextlib.suppress(Exception):
                conn.terminate()

        return stop

    async def close(self) -> None:
        await self._pool.close()


async def create_pool_executor(
    dsn: str,
    *,
    min_size: int = 1,
    max_size: int = 10,
    schema: str | None = None,
) -> PgExecutor:
    """Build the built-in asyncpg-backed executor.

    ``schema`` puts cairnq's tables in a schema of their own rather than in
    whatever the connection's search_path leads with. The protocol's SQL names no
    schema, so this is a connection setting and not one statement changes.
    """
    try:
        import asyncpg
    except ImportError as e:  # pragma: no cover - import guard
        raise RuntimeError(
            "PostgresStore requires asyncpg — install cairnq[postgres]"
        ) from e
    if schema is not None:
        check_schema_name(schema)
    pool = await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        # A startup parameter, so every connection this pool ever hands out
        # resolves in the right schema — migrations included. Setting it to a
        # schema that does not exist yet is legal; the CREATE below is what makes
        # it resolve.
        #
        # A startup parameter is what the TypeScript twin wanted and could not
        # have — `pg` lets a DSN's own `?options=` replace it, so that SDK sets
        # search_path per connection instead. See pg-pool.ts.
        #
        # Quoted, like the CREATE below and like that `set search_path`: search_path is a GUC list, and an unquoted item is folded
        # to lower case. PLAIN_IDENT admits upper case, so `schema="MySchema"`
        # would create "MySchema" and then resolve to `myschema` — a store that
        # fails _check_schema for a configuration that is spelled correctly.
        server_settings={"search_path": f'"{schema}"'} if schema else {},
    )
    if schema:
        # Created here rather than from the migrations (which name no schema, by
        # design). Without it the first `create table` fails with "no schema has
        # been selected to create in", which says nothing about the cause.
        try:
            await pool.execute(f'create schema if not exists "{schema}"')
        except BaseException:
            await pool.close()  # never leak a pool we made
            raise
    return _PoolExecutor(asyncpg, pool, dsn)
