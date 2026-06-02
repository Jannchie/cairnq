"""PostgresStore — asyncpg backend executing the shared cairnq-protocol SQL
(postgres dialect). Multi-host capable: unlike SQLite this coordinates API and
worker processes across machines through one Postgres database. Time comes from
the DB clock (now()), claim uses FOR UPDATE SKIP LOCKED, JSON columns are jsonb
(bound as JSON text, read back as objects by Task.from_row's adaptive parse —
asyncpg returns jsonb as str by default).

asyncpg is an optional dependency (install ``cairnq[postgres]``); it is imported
lazily in __init__ so the rest of the SDK works without it."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from .._ids import new_id
from .._sql import load_migrations, load_statements
from ..errors import AlreadyExists, LostLease, ProtocolVersionMismatch, error_envelope
from ..models import Task
from .base import TaskStore

SUPPORTED_PROTOCOL_MAJOR = 1

LEASE_EXPIRED_ERROR_JSON = json.dumps(
    error_envelope(
        type="LeaseExpired",
        code="lease_expired",
        message="task lease expired and max attempts reached",
        retryable=False,
    )
)

_NAMED = re.compile(r"(?<!:):(\w+)")
_COMMENT = re.compile(r"--[^\n]*")


def to_positional(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """Translate the protocol's named-parameter SQL (`:name`) into asyncpg
    positional placeholders (`$1`), collapsing each DISTINCT name to ONE slot —
    statements reuse a name across CASE branches / IS NULL guards (e.g. list.sql).
    Comments are stripped first so a `:name` in a header comment (e.g. "now +
    :lease_ms") never leaks into the parameter list."""
    body = _COMMENT.sub("", sql)
    order: list[str] = []
    slot: dict[str, int] = {}

    def repl(m: "re.Match[str]") -> str:
        name = m.group(1)
        if name not in slot:
            order.append(name)
            slot[name] = len(order)  # 1-based $n
        return f"${slot[name]}"

    text = _NAMED.sub(repl, body)
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
                            f"storage protocol_version={version}, SDK supports {SUPPORTED_PROTOCOL_MAJOR}"
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
        applied = {r["name"] for r in await conn.fetch("select name from cairnq_migrations")}
        for name, sql in load_migrations("postgres"):
            if name in applied:
                continue
            async with conn.transaction():
                await conn.execute(sql)  # multi-statement DDL
                await conn.execute(
                    "insert into cairnq_migrations (name, applied_at_ms) values "
                    "($1, (extract(epoch from now()) * 1000)::bigint) on conflict (name) do nothing",
                    name,
                )

    async def close(self) -> None:
        if self._pool is not None:
            pool = self._pool
            self._pool = None
            await pool.close()

    async def _read_protocol_version(self, conn: Any) -> int:
        row = await conn.fetchrow(
            "select value from cairnq_meta where key = 'protocol_version'"
        )
        return int(row["value"]) if row else 0

    async def protocol_version(self) -> int:
        if self._pool is None:
            await self.connect()
        async with self._pool.acquire() as conn:
            return await self._read_protocol_version(conn)

    # --------------------------------------------------------------- internals
    async def _run(self, name: str, params: dict[str, Any]) -> list[Any]:
        text, values = to_positional(self._sql[name], params)
        async with self._pool.acquire() as conn:
            return await conn.fetch(text, *values)

    async def _run_on(self, conn: Any, name: str, params: dict[str, Any]) -> list[Any]:
        text, values = to_positional(self._sql[name], params)
        return await conn.fetch(text, *values)

    async def _owned_write(self, name: str, task_id: str, params: dict[str, Any]) -> Task:
        """Ownership-checked worker write: each statement's WHERE pins worker_id +
        a live lease, so 0 rows back means the lease was lost."""
        rows = await self._run(name, params)
        if not rows:
            raise LostLease(task_id)
        return Task.from_row(rows[0])

    # ----------------------------------------------------------- client side
    async def submit(
        self,
        *,
        name: str,
        payload: dict[str, Any] | None = None,
        queue: str = "default",
        key: str | None = None,
        conflict: str = "reuse",
        max_attempts: int = 3,
        priority: int = 0,
        metadata: dict[str, Any] | None = None,
        parent_id: str | None = None,
        root_id: str | None = None,
        correlation_id: str | None = None,
        run_at_delay_ms: int = 0,
    ) -> Task:
        if self._pool is None:
            await self.connect()
        task_id = new_id("task")
        # No now_ms / run_at_ms: the DB clock supplies time, so submit passes a
        # relative :delay_ms and the SQL computes run_at = now + delay.
        ins = {
            "id": task_id,
            "name": name,
            "queue": queue,
            "payload": json.dumps(payload if payload is not None else {}),
            "metadata": json.dumps(metadata or {}),
            "max_attempts": max_attempts,
            "priority": priority,
            "delay_ms": run_at_delay_ms,
            "parent_id": parent_id,
            "root_id": root_id or task_id,
            "correlation_id": correlation_id,
        }
        if key is None:
            rows = await self._run("insert_task", ins)
            return Task.from_row(rows[0])

        async with self._pool.acquire() as conn, conn.transaction():
            existing = await self._run_on(conn, "get_key", {"key": key})
            if existing:
                ex_id = existing[0]["task_id"]
                if conflict == "reuse":
                    ex = await self._run_on(conn, "get", {"id": ex_id})
                    return Task.from_row(ex[0])
                if conflict == "reject":
                    raise AlreadyExists(key)
                if conflict == "replace":
                    await self._run_on(conn, "cancel", {"id": ex_id})
                    rows = await self._run_on(conn, "insert_task", ins)
                    await self._run_on(conn, "upsert_key", {"key": key, "task_id": task_id})
                    return Task.from_row(rows[0])
                raise ValueError(f"unknown conflict strategy: {conflict!r}")

            rows = await self._run_on(conn, "insert_task", ins)
            await self._run_on(conn, "upsert_key", {"key": key, "task_id": task_id})
            return Task.from_row(rows[0])

    async def get(self, task_id: str) -> Task | None:
        if self._pool is None:
            await self.connect()
        rows = await self._run("get", {"id": task_id})
        return Task.from_row(rows[0]) if rows else None

    async def get_by_key(self, key: str) -> Task | None:
        if self._pool is None:
            await self.connect()
        rows = await self._run("get_by_key", {"key": key})
        return Task.from_row(rows[0]) if rows else None

    async def list(
        self,
        *,
        status: str | None = None,
        queue: str | None = None,
        name: str | None = None,
        root_id: str | None = None,
        correlation_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        if self._pool is None:
            await self.connect()
        params = {
            "status": status,
            "queue": queue,
            "name": name,
            "root_id": root_id,
            "correlation_id": correlation_id,
            "limit": limit,
            "offset": offset,
        }
        rows = await self._run("list", params)
        return [Task.from_row(r) for r in rows]

    async def cancel(self, task_id: str) -> Task | None:
        if self._pool is None:
            await self.connect()
        rows = await self._run("cancel", {"id": task_id})
        return Task.from_row(rows[0]) if rows else None

    async def cancel_by_key(self, key: str) -> Task | None:
        if self._pool is None:
            await self.connect()
        async with self._pool.acquire() as conn, conn.transaction():
            existing = await self._run_on(conn, "get_key", {"key": key})
            if not existing:
                return None
            rows = await self._run_on(conn, "cancel", {"id": existing[0]["task_id"]})
            return Task.from_row(rows[0]) if rows else None

    async def retry(self, task_id: str, *, reset_attempt: bool = False) -> Task | None:
        if self._pool is None:
            await self.connect()
        rows = await self._run("retry", {"id": task_id, "reset_attempt": reset_attempt})
        return Task.from_row(rows[0]) if rows else None

    async def retry_by_key(self, key: str, *, reset_attempt: bool = False) -> Task | None:
        if self._pool is None:
            await self.connect()
        async with self._pool.acquire() as conn, conn.transaction():
            existing = await self._run_on(conn, "get_key", {"key": key})
            if not existing:
                return None
            rows = await self._run_on(
                conn, "retry", {"id": existing[0]["task_id"], "reset_attempt": reset_attempt}
            )
            return Task.from_row(rows[0]) if rows else None

    # ----------------------------------------------------------- worker side
    async def claim(
        self, *, queues: list[str], worker_id: str, lease_ms: int = 30_000, limit: int = 1
    ) -> list[Task]:
        if self._pool is None:
            await self.connect()
        # No claimable_probe: PG readers don't block writers, so a plain transaction
        # (recover then claim) is cheap even when idle. FOR UPDATE SKIP LOCKED in
        # claim.sql gives true concurrent, non-contending dispatch.
        async with self._pool.acquire() as conn, conn.transaction():
            await self._run_on(
                conn, "recover_leases", {"lease_expired_error": LEASE_EXPIRED_ERROR_JSON}
            )
            rows = await self._run_on(
                conn,
                "claim",
                {
                    "queues": list(queues),
                    "worker_id": worker_id,
                    "lease_ms": lease_ms,
                    "limit": limit,
                },
            )
            return [Task.from_row(r) for r in rows]

    async def heartbeat(self, *, task_id: str, worker_id: str, lease_ms: int = 30_000) -> Task:
        if self._pool is None:
            await self.connect()
        return await self._owned_write(
            "heartbeat", task_id, {"id": task_id, "worker_id": worker_id, "lease_ms": lease_ms}
        )

    async def progress(
        self, *, task_id: str, worker_id: str, progress: float | None, message: str | None
    ) -> Task:
        if self._pool is None:
            await self.connect()
        return await self._owned_write(
            "progress",
            task_id,
            {"id": task_id, "worker_id": worker_id, "progress": progress, "message": message},
        )

    async def succeed(self, *, task_id: str, worker_id: str, result: Any) -> Task:
        if self._pool is None:
            await self.connect()
        return await self._owned_write(
            "succeed",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "result": json.dumps(result) if result is not None else None,
                "message": None,
            },
        )

    async def complete(self, *, task_id: str, worker_id: str, result: Any) -> Task:
        if self._pool is None:
            await self.connect()
        return await self._owned_write(
            "complete",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "result": json.dumps(result) if result is not None else None,
            },
        )

    async def fail(
        self,
        *,
        task_id: str,
        worker_id: str,
        error: dict[str, Any],
        retryable: bool = True,
        delay_ms: int = 0,
    ) -> Task:
        if self._pool is None:
            await self.connect()
        return await self._owned_write(
            "fail",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "error": json.dumps(error),
                "retryable": retryable,
                "delay_ms": delay_ms,
            },
        )
