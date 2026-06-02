"""Error types. The wire protocol only carries JSON error envelopes
(`{type, code, message, retryable, details}`); these Python exceptions wrap them
at the SDK boundary."""

from __future__ import annotations

from typing import Any


def error_envelope(
    *,
    type: str,
    code: str,
    message: str,
    retryable: bool,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The single shape of the JSON error envelope (see PROTOCOL.md). Everything
    that records an error — a handler exception, a missing handler, lease expiry,
    a raised TaskError — builds it here, so the contract's fields live in one place."""
    return {
        "type": type,
        "code": code,
        "message": message,
        "retryable": retryable,
        "details": details or {},
    }


class CairnQError(Exception):
    """Base for all CairnQ SDK errors."""


class AlreadyExists(CairnQError):
    """submit with conflict=reject hit an existing key."""

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"task with key {key!r} already exists")


class TaskTimeout(CairnQError):
    """wait/call did not reach a terminal status within the timeout. The task
    keeps running; `task_id` lets the caller follow up."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"task {task_id} did not finish in time")


class TaskFailed(CairnQError):
    """A waited-on task ended in `failed`. The envelope's fields are unpacked onto
    the exception — read `e.code` / `e.message` / `e.retryable` / `e.details` instead
    of indexing `e.error` (the raw envelope stays available on `e.error`)."""

    def __init__(self, error: dict[str, Any] | None):
        self.error = error or {}
        self.type = self.error.get("type", "TaskError")
        self.code = self.error.get("code", "task_error")
        self.message = self.error.get("message", "task failed")
        self.retryable = bool(self.error.get("retryable", False))
        self.details = self.error.get("details") or {}
        super().__init__(self.message)


class TaskCanceled(CairnQError):
    """A waited-on task ended in `canceled`."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"task {task_id} was canceled")


class LostLease(CairnQError):
    """A worker write affected 0 rows: the lease expired and was reclaimed.
    The worker must stop touching this task."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"lost lease on task {task_id}")


class ProtocolVersionMismatch(CairnQError):
    """The storage protocol major version is incompatible with this SDK."""


class TaskError(CairnQError):
    """Raise inside a task handler to control how the failure is recorded.

    By default (`retryable=False`) the task fails permanently instead of burning
    retries on a deterministic error. Set `retryable=True` for transient errors
    you do want re-attempted. Any *other* exception a handler raises is treated as
    retryable."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "task_error",
        retryable: bool = False,
        type: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        self.code = code
        self.retryable = retryable
        self.type = type or "TaskError"
        self.details = details or {}
        super().__init__(message)

    def envelope(self) -> dict[str, Any]:
        return error_envelope(
            type=self.type,
            code=self.code,
            message=str(self),
            retryable=self.retryable,
            details=self.details,
        )
