"""Error types. The wire protocol only carries JSON error envelopes
(`{type, code, message, retryable, details}`); these Python exceptions wrap them
at the SDK boundary."""

from __future__ import annotations

from typing import Any

from ._ids import now_ms
from .models import Task


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


class QueueFull(CairnQError):
    """A gated submit waited out `max_queue_wait_ms` without the queue draining
    below its depth limit. Nothing was enqueued.

    Distinct from a slow submit on purpose: a queue this far behind is a capacity
    problem, and a caller that silently retries forever converts it into an
    invisible one."""

    def __init__(self, queue: str, max_depth: int, waited_ms: int):
        self.queue = queue
        self.max_depth = max_depth
        self.waited_ms = waited_ms
        super().__init__(
            f"queue {queue!r} still holds {max_depth} or more queued tasks after "
            f"{waited_ms}ms; refusing to enqueue more"
        )


def _timeout_detail(task: Task | None) -> str:
    """One line of "why hasn't this finished" from the last snapshot wait()
    observed. No worker running, no handler for the name, wrong queue, and two
    processes on different database files all look identical from the API side —
    queued, never claimed — so that case names the likely causes."""
    if task is None:
        return "task not found — wrong database file, or already purged?"
    if task.queued:
        delay_ms = task.run_at_ms - now_ms()
        if task.attempt == 0 and delay_ms <= 0:
            return (
                "never claimed by a worker — is a worker running with a handler for "
                f"'{task.name}' on queue '{task.queue}', against this same database?"
            )
        next_run = f", next run in ~{delay_ms}ms" if delay_ms > 0 else ""
        return f"still queued (attempt {task.attempt}/{task.max_attempts}){next_run}"
    if task.cancel_requested:
        return "cancel requested, waiting for the handler to observe it"
    return f"still running (attempt {task.attempt}/{task.max_attempts})"


class TaskTimeout(CairnQError):
    """wait/call did not reach a terminal status within the timeout. The task
    keeps running; `task_id` lets the caller follow up. `task` is the last
    snapshot wait() observed (None if get() found nothing), and the message says
    what state it was stuck in — a queued-never-claimed task is the classic
    first-run failure (no worker, no handler, wrong queue or file)."""

    def __init__(self, task_id: str, *, timeout_ms: int | None = None, task: Task | None = None):
        self.task_id = task_id
        self.task = task
        message = (
            f"task {task_id} did not finish in time"
            if timeout_ms is None
            else f"task {task_id} did not finish within {timeout_ms}ms: {_timeout_detail(task)}"
        )
        super().__init__(message)


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


class SerializationError(CairnQError):
    """A value could not be encoded for a protocol JSON column (non-finite
    number, set, datetime, …). Raised at the boundary — submit raises it, and a
    worker records a handler result that triggers it as a permanent
    `unserializable_result` failure. The TypeScript SDK raises the same named
    error."""


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
