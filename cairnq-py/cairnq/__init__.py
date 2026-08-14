"""CairnQ — SQLite-first, cross-language, storage-centered durable task runtime."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from .backpressure import QueueDepthGate, QueueDepthLimit
from .client import CairnQ
from .context import TaskContext
from .errors import (
    AlreadyExists,
    CairnQError,
    EventLoopBlocked,
    LostLease,
    ProtocolVersionMismatch,
    QueueFull,
    SerializationError,
    TaskCanceled,
    TaskError,
    TaskFailed,
    TaskTimeout,
)
from .models import STATUSES, Task, TaskDef, TaskRef, TaskStatus
from .retention import Retention, RetentionSweeper
from .store import Conflict, PostgresStore, SQLiteStore, TaskStore
from .worker import Worker

# Read from the installed package metadata rather than repeated here: the version
# lives in pyproject.toml alone, so a release bump can't leave this behind.
try:
    __version__ = _pkg_version("cairnq")
except PackageNotFoundError:  # source tree without an install
    __version__ = "0.0.0.dev0"

__all__ = [
    "CairnQ",
    "Worker",
    "TaskContext",
    "Task",
    "TaskDef",
    "TaskRef",
    "TaskStatus",
    "STATUSES",
    "Conflict",
    "TaskStore",
    "SQLiteStore",
    "PostgresStore",
    "QueueDepthGate",
    "QueueDepthLimit",
    "Retention",
    "RetentionSweeper",
    "CairnQError",
    "AlreadyExists",
    "QueueFull",
    "TaskTimeout",
    "TaskFailed",
    "TaskCanceled",
    "TaskError",
    "LostLease",
    "EventLoopBlocked",
    "ProtocolVersionMismatch",
    "SerializationError",
]
