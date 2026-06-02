"""The storage seam. SQLiteStore is the only MVP implementation; PostgresStore /
MemoryStore can slot in later behind this same interface. Users never touch a
TaskStore directly — they use CairnQ / Worker / TaskContext."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..models import Task


class TaskStore(ABC):
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def protocol_version(self) -> int: ...

    # --- client side ---
    @abstractmethod
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
    ) -> Task: ...

    @abstractmethod
    async def get(self, task_id: str) -> Task | None: ...

    @abstractmethod
    async def get_by_key(self, key: str) -> Task | None: ...

    @abstractmethod
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
    ) -> list[Task]: ...

    @abstractmethod
    async def cancel(self, task_id: str) -> Task | None: ...

    @abstractmethod
    async def cancel_by_key(self, key: str) -> Task | None: ...

    @abstractmethod
    async def retry(self, task_id: str, *, reset_attempt: bool = False) -> Task | None: ...

    @abstractmethod
    async def retry_by_key(self, key: str, *, reset_attempt: bool = False) -> Task | None: ...

    # --- worker side ---
    @abstractmethod
    async def claim(
        self, *, queues: list[str], worker_id: str, lease_ms: int = 30_000, limit: int = 1
    ) -> list[Task]: ...

    @abstractmethod
    async def heartbeat(self, *, task_id: str, worker_id: str, lease_ms: int = 30_000) -> Task: ...

    @abstractmethod
    async def progress(
        self, *, task_id: str, worker_id: str, progress: float | None, message: str | None
    ) -> Task: ...

    @abstractmethod
    async def succeed(self, *, task_id: str, worker_id: str, result: Any) -> Task: ...

    @abstractmethod
    async def complete(self, *, task_id: str, worker_id: str, result: Any) -> Task: ...

    @abstractmethod
    async def fail(
        self,
        *,
        task_id: str,
        worker_id: str,
        error: dict[str, Any],
        retryable: bool = True,
        delay_ms: int = 0,
    ) -> Task: ...
