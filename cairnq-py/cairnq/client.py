from __future__ import annotations

from typing import Any

from ._wait import DEFAULT_POLL_MS, poll_wait
from .errors import TaskCanceled, TaskFailed
from .models import Task, TaskDef, TaskStatus, task_name
from .store.base import Conflict, TaskStore
from .store.postgres import PostgresStore
from .store.sqlite import SQLiteStore


class CairnQ:
    """The API-side handle. Thin wrapper over a TaskStore plus SDK-orchestrated
    wait/call polling."""

    def __init__(self, store: TaskStore):
        self._store = store

    @classmethod
    def sqlite(cls, path: str, **kwargs: Any) -> "CairnQ":
        return cls(SQLiteStore(path, **kwargs))

    @classmethod
    def postgres(cls, dsn: str, **kwargs: Any) -> "CairnQ":
        """Multi-host backend. `dsn` is a libpq connection string; requires the
        optional asyncpg package (install cairnq[postgres])."""
        return cls(PostgresStore(dsn, **kwargs))

    @property
    def store(self) -> TaskStore:
        return self._store

    async def connect(self) -> None:
        await self._store.connect()

    async def close(self) -> None:
        await self._store.close()

    async def submit(
        self,
        name: str | TaskDef[Any, Any],
        payload: dict[str, Any] | None = None,
        *,
        key: str | None = None,
        queue: str = "default",
        conflict: Conflict = "reuse",
        max_attempts: int = 3,
        priority: int = 0,
        metadata: dict[str, Any] | None = None,
        parent_id: str | None = None,
        root_id: str | None = None,
        correlation_id: str | None = None,
        run_at_delay_ms: int = 0,
    ) -> Task:
        return await self._store.submit(
            name=task_name(name),
            payload=payload,
            key=key,
            queue=queue,
            conflict=conflict,
            max_attempts=max_attempts,
            priority=priority,
            metadata=metadata,
            parent_id=parent_id,
            root_id=root_id,
            correlation_id=correlation_id,
            run_at_delay_ms=run_at_delay_ms,
        )

    async def get(self, task_id: str) -> Task | None:
        return await self._store.get(task_id)

    async def get_by_key(self, key: str) -> Task | None:
        return await self._store.get_by_key(key)

    async def list(
        self,
        *,
        status: TaskStatus | None = None,
        queue: str | None = None,
        name: str | None = None,
        root_id: str | None = None,
        correlation_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        return await self._store.list(
            status=status,
            queue=queue,
            name=name,
            root_id=root_id,
            correlation_id=correlation_id,
            limit=limit,
            offset=offset,
        )

    async def cancel(self, task_id: str) -> Task | None:
        return await self._store.cancel(task_id)

    async def cancel_by_key(self, key: str) -> Task | None:
        return await self._store.cancel_by_key(key)

    async def retry(self, task_id: str, *, reset_attempt: bool = False) -> Task | None:
        return await self._store.retry(task_id, reset_attempt=reset_attempt)

    async def retry_by_key(self, key: str, *, reset_attempt: bool = False) -> Task | None:
        return await self._store.retry_by_key(key, reset_attempt=reset_attempt)

    async def purge(self, *, older_than_ms: int = 0, limit: int = 1_000) -> list[str]:
        """Delete terminal tasks that finished more than `older_than_ms` ago and
        return their ids. Nothing else in CairnQ removes rows, so a long-lived
        database needs this on a schedule. Each call is bounded by `limit` to keep
        the write short; loop until it returns fewer than `limit`."""
        return await self._store.purge(older_than_ms=older_than_ms, limit=limit)

    async def stats(self) -> dict[str, dict[TaskStatus, int]]:
        """Task counts per queue, keyed by status and zero-filled across all
        statuses — `stats()["default"]["queued"]` is the backlog of a queue."""
        return await self._store.stats()

    async def wait(
        self, task_id: str, *, timeout_ms: int = 30_000, poll_ms: int = DEFAULT_POLL_MS
    ) -> Task:
        return await poll_wait(self._store, task_id, timeout_ms=timeout_ms, poll_ms=poll_ms)

    async def call(
        self,
        name: str | TaskDef[Any, Any],
        payload: dict[str, Any] | None = None,
        *,
        wait_timeout_ms: int = 30_000,
        poll_ms: int = DEFAULT_POLL_MS,
        **submit_kwargs: Any,
    ) -> Any:
        """submit + wait. Returns the result on success; raises TaskFailed /
        TaskCanceled / TaskTimeout otherwise. On timeout the task keeps running.
        Accepts a name string or a TaskDef (its name is used)."""
        task = await self.submit(name, payload, **submit_kwargs)
        final = await self.wait(task.id, timeout_ms=wait_timeout_ms, poll_ms=poll_ms)
        if final.succeeded:
            return final.result
        if final.failed:
            raise TaskFailed(final.error)
        raise TaskCanceled(final.id)

    async def __aenter__(self) -> "CairnQ":
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
