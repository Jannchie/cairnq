"""SQLiteStore — aiosqlite backend that executes the shared cairnq-protocol SQL.

Concurrency model: one connection guarded by an asyncio.Lock. SQLite is a single
writer anyway, and the lock prevents multi-statement transactions (submit-with-key,
claim+recover) from interleaving on the shared connection. The lock is only held
for short DB work — never while a task handler runs. Cross-process contention
(deployment mode B) is absorbed by busy_timeout."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import aiosqlite

from .._ids import new_id, now_ms
from .._sql import load_migrations, load_statements
from ..errors import AlreadyExists, LostLease, ProtocolVersionMismatch, error_envelope
from ..models import Task
from .base import TaskStore

SUPPORTED_PROTOCOL_MAJOR = 1

LEASE_EXPIRED_ERROR = error_envelope(
    type="LeaseExpired",
    code="lease_expired",
    message="task lease expired and max attempts reached",
    retryable=False,
)
# Serialized once: it's an immutable constant bound on every claim that finds work.
LEASE_EXPIRED_ERROR_JSON = json.dumps(LEASE_EXPIRED_ERROR)


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
            await self._conn.close()
            self._conn = None

    async def _ensure(self) -> aiosqlite.Connection:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None
        return self._conn

    async def _check_version(self) -> None:
        cur = await self._conn.execute(
            "select value from cairnq_meta where key = 'protocol_version'"
        )
        row = await cur.fetchone()
        await cur.close()
        version = int(row["value"]) if row else 0
        if version != SUPPORTED_PROTOCOL_MAJOR:
            raise ProtocolVersionMismatch(
                f"storage protocol_version={version}, SDK supports {SUPPORTED_PROTOCOL_MAJOR}"
            )

    async def protocol_version(self) -> int:
        await self._ensure()
        cur = await self._conn.execute(
            "select value from cairnq_meta where key = 'protocol_version'"
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row["value"]) if row else 0

    # --------------------------------------------------------------- internals
    async def _run(self, name: str, params: dict[str, Any]) -> list[aiosqlite.Row]:
        cur = await self._conn.execute(self._sql[name], params)
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def _begin(self) -> None:
        await self._conn.execute("BEGIN IMMEDIATE")

    async def _rollback_quietly(self) -> None:
        try:
            await self._conn.execute("ROLLBACK")
        except Exception:
            pass

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncIterator[None]:
        """BEGIN IMMEDIATE … COMMIT, rolling back on any error. Mirrors the TS
        SDK's db.transaction() so multi-statement ops read the same in both SDKs
        and drop the per-method commit/rollback bookkeeping."""
        await self._begin()
        try:
            yield
        except BaseException:
            await self._rollback_quietly()
            raise
        else:
            await self._conn.execute("COMMIT")

    async def _owned_write(self, name: str, task_id: str, params: dict[str, Any]) -> Task:
        """An ownership-checked worker write (heartbeat/progress/succeed/complete/
        fail). Each statement's WHERE pins worker_id + a live lease, so 0 rows back
        means the lease was lost — every such write reports it the same way."""
        async with self._lock:
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
        await self._ensure()
        now = now_ms()
        task_id = new_id("task")
        ins = {
            "id": task_id,
            "name": name,
            "queue": queue,
            "payload": json.dumps(payload if payload is not None else {}),
            "metadata": json.dumps(metadata or {}),
            "max_attempts": max_attempts,
            "priority": priority,
            "run_at_ms": now + run_at_delay_ms,
            "parent_id": parent_id,
            "root_id": root_id or task_id,
            "correlation_id": correlation_id,
            "now_ms": now,
        }
        async with self._lock, self._transaction():
            if key is None:
                rows = await self._run("insert_task", ins)
                return Task.from_row(rows[0])

            existing = await self._run("get_key", {"key": key})
            if existing:
                ex_id = existing[0]["task_id"]
                if conflict == "reuse":
                    ex = await self._run("get", {"id": ex_id})
                    return Task.from_row(ex[0])
                if conflict == "reject":
                    raise AlreadyExists(key)
                if conflict == "replace":
                    await self._run("cancel", {"id": ex_id, "now_ms": now})
                    rows = await self._run("insert_task", ins)
                    await self._run("upsert_key", {"key": key, "task_id": task_id, "now_ms": now})
                    return Task.from_row(rows[0])
                raise ValueError(f"unknown conflict strategy: {conflict!r}")

            rows = await self._run("insert_task", ins)
            await self._run("upsert_key", {"key": key, "task_id": task_id, "now_ms": now})
            return Task.from_row(rows[0])

    async def get(self, task_id: str) -> Task | None:
        await self._ensure()
        async with self._lock:
            rows = await self._run("get", {"id": task_id})
        return Task.from_row(rows[0]) if rows else None

    async def get_by_key(self, key: str) -> Task | None:
        await self._ensure()
        async with self._lock:
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
        await self._ensure()
        params = {
            "status": status,
            "queue": queue,
            "name": name,
            "root_id": root_id,
            "correlation_id": correlation_id,
            "limit": limit,
            "offset": offset,
        }
        async with self._lock:
            rows = await self._run("list", params)
        return [Task.from_row(r) for r in rows]

    async def cancel(self, task_id: str) -> Task | None:
        await self._ensure()
        async with self._lock:
            rows = await self._run("cancel", {"id": task_id, "now_ms": now_ms()})
        return Task.from_row(rows[0]) if rows else None

    async def cancel_by_key(self, key: str) -> Task | None:
        await self._ensure()
        async with self._lock, self._transaction():
            existing = await self._run("get_key", {"key": key})
            if not existing:
                return None
            rows = await self._run("cancel", {"id": existing[0]["task_id"], "now_ms": now_ms()})
            return Task.from_row(rows[0]) if rows else None

    async def retry(self, task_id: str, *, reset_attempt: bool = False) -> Task | None:
        await self._ensure()
        params = {"id": task_id, "now_ms": now_ms(), "reset_attempt": 1 if reset_attempt else 0}
        async with self._lock:
            rows = await self._run("retry", params)
        return Task.from_row(rows[0]) if rows else None

    async def retry_by_key(self, key: str, *, reset_attempt: bool = False) -> Task | None:
        await self._ensure()
        async with self._lock, self._transaction():
            existing = await self._run("get_key", {"key": key})
            if not existing:
                return None
            rows = await self._run(
                "retry",
                {
                    "id": existing[0]["task_id"],
                    "now_ms": now_ms(),
                    "reset_attempt": 1 if reset_attempt else 0,
                },
            )
            return Task.from_row(rows[0]) if rows else None

    # ----------------------------------------------------------- worker side
    async def claim(
        self, *, queues: list[str], worker_id: str, lease_ms: int = 30_000, limit: int = 1
    ) -> list[Task]:
        await self._ensure()
        now = now_ms()
        queues_json = json.dumps(list(queues))
        async with self._lock:
            # Read-only probe first: skip the write lock entirely when idle.
            probe = await self._run("claimable_probe", {"queues": queues_json, "now_ms": now})
            if not probe or not probe[0]["has_work"]:
                return []
            async with self._transaction():
                await self._run(
                    "recover_leases",
                    {"now_ms": now, "lease_expired_error": LEASE_EXPIRED_ERROR_JSON},
                )
                rows = await self._run(
                    "claim",
                    {
                        "queues": queues_json,
                        "now_ms": now,
                        "worker_id": worker_id,
                        "lease_until_ms": now + lease_ms,
                        "limit": limit,
                    },
                )
                return [Task.from_row(r) for r in rows]

    async def heartbeat(self, *, task_id: str, worker_id: str, lease_ms: int = 30_000) -> Task:
        await self._ensure()
        now = now_ms()
        return await self._owned_write(
            "heartbeat",
            task_id,
            {"id": task_id, "worker_id": worker_id, "now_ms": now, "lease_until_ms": now + lease_ms},
        )

    async def progress(
        self, *, task_id: str, worker_id: str, progress: float | None, message: str | None
    ) -> Task:
        await self._ensure()
        return await self._owned_write(
            "progress",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "now_ms": now_ms(),
                "progress": progress,
                "message": message,
            },
        )

    async def succeed(self, *, task_id: str, worker_id: str, result: Any) -> Task:
        await self._ensure()
        return await self._owned_write(
            "succeed",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "now_ms": now_ms(),
                "result": json.dumps(result) if result is not None else None,
                "message": None,
            },
        )

    async def complete(self, *, task_id: str, worker_id: str, result: Any) -> Task:
        await self._ensure()
        return await self._owned_write(
            "complete",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "now_ms": now_ms(),
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
        await self._ensure()
        return await self._owned_write(
            "fail",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "now_ms": now_ms(),
                "error": json.dumps(error),
                "retryable": 1 if retryable else 0,
                "delay_ms": delay_ms,
            },
        )
