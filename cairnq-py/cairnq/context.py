from __future__ import annotations

from typing import Any

from ._wait import poll_wait
from .models import Task, TaskDef, task_name
from .store.base import TaskStore


class TaskContext:
    """Handed to a task handler. Worker-side capabilities mirror the TS SDK."""

    def __init__(self, store: TaskStore, task: Task, worker_id: str, lease_ms: int):
        self._store = store
        self._task = task
        self._worker_id = worker_id
        self._lease_ms = lease_ms

    @property
    def task_id(self) -> str:
        return self._task.id

    @property
    def name(self) -> str:
        return self._task.name

    @property
    def queue(self) -> str:
        return self._task.queue

    @property
    def attempt(self) -> int:
        return self._task.attempt

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def metadata(self) -> dict[str, Any]:
        return self._task.metadata

    @property
    def root_id(self) -> str | None:
        return self._task.root_id

    @property
    def correlation_id(self) -> str | None:
        return self._task.correlation_id

    @property
    def payload(self) -> dict[str, Any]:
        return self._task.payload

    async def progress(self, value: float | None, message: str | None = None) -> Task:
        return await self._store.progress(
            task_id=self._task.id, worker_id=self._worker_id, progress=value, message=message
        )

    async def heartbeat(self) -> Task:
        return await self._store.heartbeat(
            task_id=self._task.id, worker_id=self._worker_id, lease_ms=self._lease_ms
        )

    async def canceled(self) -> bool:
        """Cooperative cancel check. Reads the current row (cheap) and reports
        whether a cancel was requested or the task was already canceled."""
        task = await self._store.get(self._task.id)
        if task is None:
            return True
        return task.cancel_requested or task.status == "canceled"

    async def submit(
        self, name: str | TaskDef[Any, Any], payload: dict[str, Any] | None = None, **kwargs: Any
    ) -> Task:
        """Submit a child task. parent/root/correlation are wired automatically
        so the whole chain is queryable via list(root_id=...)."""
        return await self._store.submit(
            name=task_name(name),
            payload=payload,
            parent_id=self._task.id,
            root_id=self._task.root_id,
            correlation_id=self._task.correlation_id,
            **kwargs,
        )

    async def wait(self, task_id: str, *, timeout_ms: int = 30_000, poll_ms: int = 150) -> Task:
        return await poll_wait(self._store, task_id, timeout_ms=timeout_ms, poll_ms=poll_ms)
