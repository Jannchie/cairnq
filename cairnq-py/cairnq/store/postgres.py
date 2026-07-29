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
from collections.abc import AsyncIterator, Awaitable
from functools import lru_cache
from typing import Any

from .._sql import load_migrations, load_statements
from .base import COMMENT, NAMED, Fetch, TaskStore, check_protocol_version, statement_params


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
        # LISTEN/NOTIFY state. One dedicated connection listens on both channels
        # (see 0003_notify.sql). Notifications are an accelerator: every wake
        # path keeps its poll fallback, so when this connection can't be
        # established (e.g. a pooler without LISTEN support) or drops, the store
        # silently degrades to plain polling. The queued event stays set when a
        # notification arrives with nobody waiting, so a wake between polls is
        # not lost.
        self._listener: Any = None
        self._listener_state = "none"  # none | connecting | ready | closed
        self._listener_task: asyncio.Task | None = None
        self._queued_event = asyncio.Event()
        self._done_events: dict[str, asyncio.Event] = {}

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
                    check_protocol_version(await self._read_protocol_version(conn))
            except BaseException:
                await pool.close()  # never leak a pool when connect fails
                raise
            self._pool = pool  # publish only a fully-migrated, version-checked pool
            # Warm the LISTEN connection in the background so the first idle
            # sleep is already wakeable. Fire-and-forget: failure means polling.
            self._listener_ready()

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
        self._listener_state = "closed"  # no revival after close
        await self._drop_listener()
        if self._pool is not None:
            pool, self._pool = self._pool, None
            await pool.close()

    # ----------------------------------------------------------- wake channel
    def claim_wake(self, timeout_ms: int) -> Awaitable[None] | None:
        if not self._listener_ready():
            return None
        return self._claim_wake(timeout_ms)

    async def _claim_wake(self, timeout_ms: int) -> None:
        if self._queued_event.is_set():
            self._queued_event.clear()
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._queued_event.wait(), timeout_ms / 1000)
        self._queued_event.clear()

    def task_done_wake(self, task_id: str, timeout_ms: int) -> Awaitable[None] | None:
        if not self._listener_ready():
            return None
        return self._task_done_wake(task_id, timeout_ms)

    async def _task_done_wake(self, task_id: str, timeout_ms: int) -> None:
        event = self._done_events.setdefault(task_id, asyncio.Event())
        try:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(event.wait(), timeout_ms / 1000)
        finally:
            self._done_events.pop(task_id, None)

    def _listener_ready(self) -> bool:
        """True once the LISTEN connection is up; kicks off connecting it
        otherwise. Callers fall back to plain polling until it is ready (or
        forever, if it can't be established) — correctness never depends on it."""
        if self._listener_state == "ready":
            return True
        if self._listener_state == "none":
            self._listener_state = "connecting"
            self._listener_task = asyncio.get_running_loop().create_task(self._start_listener())
        return False

    async def _start_listener(self) -> None:
        try:
            conn = await self._asyncpg.connect(self._dsn)
            await conn.add_listener("cairnq_queued", self._on_notification)
            await conn.add_listener("cairnq_done", self._on_notification)
            conn.add_termination_listener(self._on_listener_lost)
            if self._listener_state == "closed":
                await conn.close()
                return
            self._listener = conn
            self._listener_state = "ready"
        except Exception:
            # Can't LISTEN here (e.g. a transaction-mode pooler). Polling covers it.
            if self._listener_state != "closed":
                self._listener_state = "closed"

    def _on_notification(self, _conn: Any, _pid: int, channel: str, payload: str) -> None:
        if channel == "cairnq_queued":
            self._queued_event.set()
        elif channel == "cairnq_done":
            event = self._done_events.get(payload)
            if event is not None:
                event.set()

    def _on_listener_lost(self, _conn: Any) -> None:
        # A dropped listener degrades to polling; the next wake call reconnects.
        if self._listener_state != "closed":
            self._listener_state = "none"
        self._listener = None
        self._release_waiters()

    async def _drop_listener(self) -> None:
        conn, self._listener = self._listener, None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()
        if self._listener_task is not None:
            self._listener_task.cancel()
            with contextlib.suppress(BaseException):
                await self._listener_task
            self._listener_task = None
        self._release_waiters()

    def _release_waiters(self) -> None:
        # Release everyone promptly; their fallback poll takes over.
        self._queued_event.set()
        for event in self._done_events.values():
            event.set()

    async def _read_protocol_version(self, conn: Any) -> int:
        # Takes an explicit conn: during connect the pool is not published yet,
        # so this cannot go through _fetch.
        text, _ = positional_statement(self._sql["protocol_version"])
        row = await conn.fetchrow(text)
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
