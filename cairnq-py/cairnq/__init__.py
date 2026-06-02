"""CairnQ — SQLite-first, cross-language, storage-centered durable task runtime."""

from __future__ import annotations

from .client import CairnQ
from .context import TaskContext
from .errors import (
    AlreadyExists,
    CairnQError,
    LostLease,
    ProtocolVersionMismatch,
    TaskCanceled,
    TaskError,
    TaskFailed,
    TaskTimeout,
)
from .models import STATUSES, Task, TaskDef, TaskStatus
from .store import PostgresStore, SQLiteStore, TaskStore
from .worker import Worker

__version__ = "0.1.0"

__all__ = [
    "CairnQ",
    "Worker",
    "TaskContext",
    "Task",
    "TaskDef",
    "TaskStatus",
    "STATUSES",
    "TaskStore",
    "SQLiteStore",
    "PostgresStore",
    "CairnQError",
    "AlreadyExists",
    "TaskTimeout",
    "TaskFailed",
    "TaskCanceled",
    "TaskError",
    "LostLease",
    "ProtocolVersionMismatch",
]
