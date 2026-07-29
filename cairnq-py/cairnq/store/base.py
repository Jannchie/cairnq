"""The storage seam.

A backend supplies three things: how to run one protocol statement, how to run
several inside a transaction, and how its dialect binds parameters. Everything
above that — the submit conflict branches, the *_by_key lookups, the recover-then-
claim sequence, the ownership-checked writes — lives here once, because those are
protocol decisions rather than storage decisions. Keeping them in one place is
what stops SQLite and Postgres from drifting apart in behavior; the shared SQL
already stops them from drifting in wording.

Users never touch a TaskStore directly — they use CairnQ / Worker / TaskContext.
"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from functools import lru_cache
from typing import Any, Literal, get_args

from .._ids import new_id
from ..errors import AlreadyExists, LostLease, ProtocolVersionMismatch, error_envelope
from ..models import Task

# Runs one named protocol statement and returns its rows.
Fetch = Callable[[str, dict[str, Any]], Awaitable[list[Any]]]

# Conflict is the canonical declaration; CONFLICTS derives from it (via
# get_args) so the runtime guard in submit() and the type can't drift apart
# (same pattern as TaskStatus/STATUSES in models.py).
Conflict = Literal["reuse", "reject", "replace"]
CONFLICTS: tuple[Conflict, ...] = get_args(Conflict)

SUPPORTED_PROTOCOL_MAJOR = 1


def check_protocol_version(version: int) -> None:
    """Refuse to run against a store whose protocol major this SDK does not
    speak. The supported major is a protocol fact, not a dialect one — every
    backend checks it here so the constant can't fork per store."""
    if version != SUPPORTED_PROTOCOL_MAJOR:
        raise ProtocolVersionMismatch(
            f"storage protocol_version={version}, SDK supports {SUPPORTED_PROTOCOL_MAJOR}"
        )


LEASE_EXPIRED_ERROR_JSON = json.dumps(
    error_envelope(
        type="LeaseExpired",
        code="lease_expired",
        message="task lease expired and max attempts reached",
        retryable=False,
    )
)

# Strips SQL line comments, so a `:name` in a header comment isn't a parameter.
COMMENT = re.compile(r"--[^\n]*")
# A `:name` placeholder. The lookbehind spares Postgres `::type` casts.
NAMED = re.compile(r"(?<!:):(\w+)")


@lru_cache(maxsize=None)
def statement_params(sql: str) -> tuple[str, ...]:
    """The parameter names a statement binds, in first-appearance order.

    Callers pass a superset of parameters and each dialect takes what its own SQL
    asks for — that is what lets one call site serve both dialects even though
    e.g. SQLite binds `:lease_until_ms` where Postgres binds `:lease_ms`. This is
    the one place that decides what counts as a parameter; both dialects' binding
    goes through it.

    Memoized on the statement text, which is loaded once and never varies: every
    dialect's binding path runs on each query, and re-scanning the SQL each time
    would put a regex sweep on the worker's poll loop.
    """
    seen: dict[str, None] = {}
    for match in NAMED.finditer(COMMENT.sub("", sql)):
        seen.setdefault(match.group(1), None)
    return tuple(seen)


class TaskStore(ABC):
    # ------------------------------------------------------------ dialect seam
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def protocol_version(self) -> int: ...

    @abstractmethod
    async def _fetch(self, name: str, params: dict[str, Any]) -> list[Any]:
        """Run one protocol statement outside a transaction, connecting if needed."""

    @abstractmethod
    def _transaction(self) -> AbstractAsyncContextManager[Fetch]:
        """Run several statements atomically; yields a Fetch bound to the txn."""

    async def _has_claimable_work(self, params: dict[str, Any]) -> bool:
        """Whether it is worth opening the claim transaction at all. SQLite gates
        its single write lock behind a read-only probe; Postgres readers don't
        block writers, so it just says yes."""
        return True

    # ---------------------------------------------------------- wake channel
    # Push wakeups are an accelerator, never a correctness mechanism: callers
    # keep their polling loop and merely cut a sleep short when a hint arrives.
    # The default (None) means "no push channel — polling is the wake
    # mechanism", which is SQLite's answer: a shared file has no reliable
    # cross-process notification primitive. PostgresStore overrides both via
    # LISTEN/NOTIFY.

    def claim_wake(self, timeout_ms: int) -> Awaitable[None] | None:
        """An awaitable resolving when a task may have become claimable, or
        after `timeout_ms` at the latest — or None when this dialect cannot
        push."""
        return None

    def task_done_wake(self, task_id: str, timeout_ms: int) -> Awaitable[None] | None:
        """Same contract for one task reaching a terminal status (wait/call)."""
        return None

    # --------------------------------------------------------------- internals
    async def _owned_write(self, name: str, task_id: str, params: dict[str, Any]) -> Task:
        """An ownership-checked worker write (heartbeat/progress/succeed/complete/
        fail). Each statement's WHERE pins worker_id + a live lease, so 0 rows back
        means the lease was lost — every such write reports it the same way."""
        rows = await self._fetch(name, params)
        if not rows:
            raise LostLease(task_id)
        return Task.from_row(rows[0])

    @staticmethod
    def _one(rows: list[Any]) -> Task | None:
        return Task.from_row(rows[0]) if rows else None

    # ------------------------------------------------------------- client side
    async def submit(
        self,
        *,
        name: str,
        payload: dict[str, Any] | None = None,
        queue: str = "default",
        key: str | None = None,
        conflict: Conflict = "reuse",
        max_attempts: int = 3,
        priority: int = 0,
        metadata: dict[str, Any] | None = None,
        parent_id: str | None = None,
        root_id: str | None = None,
        correlation_id: str | None = None,
        run_at_delay_ms: int = 0,
    ) -> Task:
        # Validate up front: untyped callers otherwise only hit the strategy
        # branch on the second submit of a key, deep inside the transaction.
        if conflict not in CONFLICTS:
            raise ValueError(f"unknown conflict strategy: {conflict!r}")
        task_id = new_id("task")
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
            return Task.from_row((await self._fetch("insert_task", ins))[0])

        # A key makes submit a read-then-write, so it has to be one transaction —
        # opened by taking the key's lock, because on Postgres the transaction
        # alone is not enough: concurrent same-key submits must not both see "no
        # existing task" (see lock_key.sql; on SQLite it is a no-op).
        async with self._transaction() as fetch:
            await fetch("lock_key", {"key": key})
            existing = await fetch("get_key", {"key": key})
            if existing:
                # Read the task itself before branching: a concurrent purge
                # (which takes no key lock) may have deleted it — cascading the
                # key row away — between our statements' snapshots. A vanished
                # task means the key is free after all, whatever the strategy.
                rows = await fetch("get", {"id": existing[0]["task_id"]})
                if rows:
                    if conflict == "reuse":
                        return Task.from_row(rows[0])
                    if conflict == "reject":
                        raise AlreadyExists(key)
                    # "replace": cancel the recorded task, then repoint the key.
                    await fetch("cancel", {"id": existing[0]["task_id"]})
            rows = await fetch("insert_task", ins)
            await fetch("upsert_key", {"key": key, "task_id": task_id})
            return Task.from_row(rows[0])

    async def get(self, task_id: str) -> Task | None:
        return self._one(await self._fetch("get", {"id": task_id}))

    async def get_by_key(self, key: str) -> Task | None:
        return self._one(await self._fetch("get_by_key", {"key": key}))

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
        rows = await self._fetch(
            "list",
            {
                "status": status,
                "queue": queue,
                "name": name,
                "root_id": root_id,
                "correlation_id": correlation_id,
                "limit": limit,
                "offset": offset,
            },
        )
        return [Task.from_row(r) for r in rows]

    async def cancel(self, task_id: str) -> Task | None:
        return self._one(await self._fetch("cancel", {"id": task_id}))

    async def retry(self, task_id: str, *, reset_attempt: bool = False) -> Task | None:
        return self._one(
            await self._fetch("retry", {"id": task_id, "reset_attempt": reset_attempt})
        )

    async def cancel_by_key(self, key: str) -> Task | None:
        return await self._by_key("cancel", key, {})

    async def retry_by_key(self, key: str, *, reset_attempt: bool = False) -> Task | None:
        return await self._by_key("retry", key, {"reset_attempt": reset_attempt})

    async def _by_key(self, name: str, key: str, params: dict[str, Any]) -> Task | None:
        """Resolve a key to the task it currently points at, then act on that task
        — under the key's lock, so a concurrent `replace` can't repoint the key
        between the lookup and the write (the transaction alone is not enough on
        Postgres; see lock_key.sql)."""
        async with self._transaction() as fetch:
            await fetch("lock_key", {"key": key})
            existing = await fetch("get_key", {"key": key})
            if not existing:
                return None
            rows = await fetch(name, {"id": existing[0]["task_id"], **params})
            return self._one(rows)

    async def purge(self, *, older_than_ms: int = 0, limit: int = 1_000) -> list[str]:
        """Delete terminal tasks that completed more than `older_than_ms` ago and
        return their ids. Nothing else removes rows, so a long-lived database
        needs this called periodically. Bounded by `limit` to keep each sweep a
        short write; call it in a loop until it returns fewer than `limit`."""
        rows = await self._fetch("purge", {"older_than_ms": older_than_ms, "limit": limit})
        return [r["id"] for r in rows]

    # ------------------------------------------------------------- worker side
    async def claim(
        self,
        *,
        queues: list[str],
        worker_id: str,
        lease_ms: int = 30_000,
        limit: int = 1,
        names: list[str] | tuple[str, ...] | None = None,
    ) -> list[Task]:
        """Take up to `limit` claimable tasks. `names` restricts the claim to task
        names this caller can actually run — a worker passes its registered
        handlers. Queues alone do not partition work, so without it a worker
        claims a task it cannot run and fails it permanently. None means no
        filter; an empty list claims nothing."""
        params = {
            "queues": list(queues),
            "names": None if names is None else list(names),
            "worker_id": worker_id,
            "lease_ms": lease_ms,
            "limit": limit,
            "lease_expired_error": LEASE_EXPIRED_ERROR_JSON,
        }
        if not await self._has_claimable_work(params):
            return []
        # Recovery must share the claim's transaction: a lease reclaimed here has
        # to be visible to the claim that follows, and to nobody in between.
        async with self._transaction() as fetch:
            await fetch("recover_leases", params)
            rows = await fetch("claim", params)
            return [Task.from_row(r) for r in rows]

    async def heartbeat(self, *, task_id: str, worker_id: str, lease_ms: int = 30_000) -> Task:
        return await self._owned_write(
            "heartbeat", task_id, {"id": task_id, "worker_id": worker_id, "lease_ms": lease_ms}
        )

    async def progress(
        self, *, task_id: str, worker_id: str, progress: float | None, message: str | None
    ) -> Task:
        return await self._owned_write(
            "progress",
            task_id,
            {"id": task_id, "worker_id": worker_id, "progress": progress, "message": message},
        )

    async def succeed(self, *, task_id: str, worker_id: str, result: Any) -> Task:
        return await self._owned_write(
            "succeed",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "result": None if result is None else json.dumps(result),
                "message": None,
            },
        )

    async def complete(self, *, task_id: str, worker_id: str, result: Any) -> Task:
        return await self._owned_write(
            "complete",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "result": None if result is None else json.dumps(result),
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
