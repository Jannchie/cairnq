from __future__ import annotations

import json
from dataclasses import dataclass, fields
from typing import Any, Generic, Literal, TypeVar, get_args

# TaskStatus is the canonical declaration within the Python SDK; STATUSES derives
# from it (via get_args) so the type and the runtime set can't drift apart. The
# cross-language source of truth is the status CHECK constraint in cairnq-protocol's
# migration, which the conformance suite pins this set against.
TaskStatus = Literal["queued", "running", "succeeded", "failed", "canceled"]
STATUSES: tuple[TaskStatus, ...] = get_args(TaskStatus)

_JSON_COLUMNS = ("payload", "result", "error", "metadata")

# The bigint columns. `attempt` / `max_attempts` / `priority` are int4 and
# `progress` is double precision, so every driver already gives those as numbers;
# only int8 has a wire form worth normalizing.
_MS_COLUMNS = (
    "lease_until_ms",
    "run_at_ms",
    "cancel_requested_at_ms",
    "created_at_ms",
    "updated_at_ms",
    "completed_at_ms",
)
TERMINAL: tuple[TaskStatus, ...] = ("succeeded", "failed", "canceled")


def is_terminal_status(status: TaskStatus) -> bool:
    """Whether a bare status is one a task stops at. `Task.is_terminal` answers
    this for a task; this answers it for a status that arrived on its own — from
    `get_status`, a `WatchSignal`, or a caller's own storage."""
    return status in TERMINAL

P = TypeVar("P")
R = TypeVar("R")


@dataclass(frozen=True)
class TaskRef:
    """The id + status pair the wait loop polls on (see get_status.sql) — a
    probe, not a snapshot: everything else about the task is deliberately not
    read. A dataclass like Task, not a tuple, so generic field access
    (`getattr`, the conformance runner) treats the two models alike."""

    id: str
    status: TaskStatus

    @classmethod
    def from_row(cls, row: Any) -> "TaskRef":
        return cls(id=row["id"], status=row["status"])

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL


@dataclass(frozen=True)
class TaskDef(Generic[P, R]):
    """A typed task handle. Define a task once and reference the same symbol from
    the worker (`@worker.task(my_task)`) and the client (`tasks.call(my_task, …)`),
    so the name lives in one place (no string drift), editors autocomplete it and
    find every caller, and `call(my_task, …)` is typed as the task's result.

    Opt-in: every API still accepts a plain name string, and cross-language callers
    keep using the string (only the name crosses the DB). The P/R type params are
    for the type checker only — nothing about them is stored or sent."""

    name: str


def task_name(task: "str | TaskDef[Any, Any]") -> str:
    """Resolve a task name from either a plain string or a TaskDef."""
    return task if isinstance(task, str) else task.name


@dataclass
class Task:
    id: str
    name: str
    queue: str
    status: TaskStatus
    payload: dict[str, Any]
    metadata: dict[str, Any]
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    progress: float | None = None
    message: str | None = None
    attempt: int = 0
    max_attempts: int = 3
    priority: int = 0
    worker_id: str | None = None
    lease_until_ms: int | None = None
    run_at_ms: int = 0
    cancel_requested_at_ms: int | None = None
    parent_id: str | None = None
    root_id: str | None = None
    correlation_id: str | None = None
    created_at_ms: int = 0
    updated_at_ms: int = 0
    completed_at_ms: int | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Task":
        d = dict(row)
        for col in _JSON_COLUMNS:
            v = d.get(col)
            # The driver decides a JSON column's wire form: SQLite (TEXT) hands
            # back a str to parse; a jsonb-aware driver hands back a decoded
            # object. Parse only a str — never assume one backend.
            d[col] = json.loads(v) if isinstance(v, str) else v
        for col in _MS_COLUMNS:
            v = d.get(col)
            # Same argument, for the other column type the drivers disagree
            # about. Postgres sends int8 down the wire as text to protect
            # precision it cannot know is unneeded; asyncpg decodes it, other
            # drivers hand back a str. Normalizing here rather than demanding it
            # of every driver is what lets an application inject a connection it
            # configured for its own needs.
            #
            # Nullability differs per column (completed_at_ms may be null,
            # created_at_ms may not), so this preserves null rather than
            # producing 0. Mirrors rowToTask in the TypeScript SDK.
            if v is not None and not isinstance(v, int):
                d[col] = int(v)
        return cls(**{k: v for k, v in d.items() if k in _TASK_FIELDS})

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL

    @property
    def cancel_requested(self) -> bool:
        return self.cancel_requested_at_ms is not None

    # Status predicates — read `if task.succeeded:` instead of memorizing the
    # status strings. Each mirrors one value of TaskStatus.
    @property
    def queued(self) -> bool:
        return self.status == "queued"

    @property
    def running(self) -> bool:
        return self.status == "running"

    @property
    def succeeded(self) -> bool:
        return self.status == "succeeded"

    @property
    def failed(self) -> bool:
        return self.status == "failed"

    @property
    def canceled(self) -> bool:
        return self.status == "canceled"


# Field-name set used by from_row to drop unknown columns; computed once here
# rather than re-introspecting the dataclass on every row mapped.
_TASK_FIELDS = frozenset(f.name for f in fields(Task))
