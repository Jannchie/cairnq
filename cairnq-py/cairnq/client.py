from __future__ import annotations

from typing import Any

from ._wait import (
    DEFAULT_POLL_MS,
    DEFAULT_WAIT_TIMEOUT_MS,
    MAX_POLL_MS,
    poll_wait,
    poll_wait_by_key,
)
from .backpressure import DEFAULT_MAX_WAIT_MS, QueueDepthLimit
from .errors import TaskCanceled, TaskFailed
from .models import Task, TaskDef, TaskRef, TaskStatus, task_name
from .retention import Retention, RetentionSweeper
from .store.pg_executor import PgExecutor
from .store.base import Conflict, TaskStore
from .store.postgres import PostgresStore
from .store.sqlite import SQLiteStore


class CairnQ:
    """The API-side handle. Thin wrapper over a TaskStore plus SDK-orchestrated
    wait/call polling."""

    def __init__(
        self,
        store: TaskStore,
        *,
        max_queue_depth: QueueDepthLimit | None = None,
        max_queue_wait_ms: int = DEFAULT_MAX_WAIT_MS,
        retention: int | Retention | None = None,
    ):
        """`retention` deletes terminal tasks on a schedule, for as long as this
        handle is open. Off unless set — and off means rows accumulate forever,
        because nothing else in CairnQ removes them.

        An int keeps every terminal row that many ms. A `Retention` is the same
        cutoff in its tiered shapes — per status, or per anything `purge` can
        filter on — plus the sweep's own knobs; see its docstring."""
        self._store = store
        # Installed on the store, not held here: every submit path goes through
        # the store, including TaskContext.submit, which this handle never sees.
        if max_queue_depth is not None:
            store.use_backpressure(max_queue_depth, max_queue_wait_ms=max_queue_wait_ms)
        # Retention goes on the store too, but for a different reason: scheduling
        # it needs a running event loop, which a handle built at import time does
        # not have. The store starts it when it connects — the one path no
        # operation can skip, since `connect()` is optional and everything
        # connects lazily through it. See TaskStore.use_retention.
        self._sweeper = (
            RetentionSweeper(
                store,
                Retention(older_than_ms=retention) if isinstance(retention, int) else retention,
            )
            if retention is not None
            else None
        )
        if self._sweeper is not None:
            store.use_retention(self._sweeper)

    # The factories name the STORE's options explicitly and forward the rest to
    # __init__ — the same split Worker.sqlite/.postgres uses, so a new client
    # option needs no registry kept in sync with this file.
    @classmethod
    def sqlite(cls, path: str, *, busy_timeout_ms: int = 5_000, **kwargs: Any) -> "CairnQ":
        return cls(SQLiteStore(path, busy_timeout_ms=busy_timeout_ms), **kwargs)

    @classmethod
    def postgres(
        cls,
        source: str | PgExecutor,
        *,
        min_size: int = 1,
        max_size: int = 10,
        schema: str | None = None,
        **kwargs: Any,
    ) -> "CairnQ":
        """Multi-host backend. `source` is a libpq connection string — which
        requires the optional asyncpg package (install cairnq[postgres]) — or a
        PgExecutor over a driver the application already runs, which cairnq then
        shares instead of opening a second pool.

        `schema` is the schema cairnq's tables live in. Given a DSN, cairnq owns
        the connection and arranges it: the schema is created if absent and set
        as the search_path. Given an executor, the connection is yours and cairnq
        only asserts that it resolves there. Every process in a deployment must
        agree on it — the TypeScript SDK takes the same option, and both refuse
        to connect where they can see the two ends have diverged."""
        return cls(
            PostgresStore(source, min_size=min_size, max_size=max_size, schema=schema),
            **kwargs,
        )

    @property
    def store(self) -> TaskStore:
        return self._store

    async def connect(self) -> None:
        await self._store.connect()

    async def close(self) -> None:
        """Stop retention (waiting for a sweep in flight, so no purge outlives
        the store) and close the store."""
        if self._sweeper is not None:
            await self._sweeper.stop()
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
        delay_ms: int = 0,
    ) -> Task:
        """Enqueue a task. With `max_queue_depth` configured this blocks while
        the target queue is at its limit, and raises QueueFull if it stays there
        for `max_queue_wait_ms` — a soft limit across several producers.

        `delay_ms` runs the task no earlier than that many ms from now."""
        return await self._store.submit(
            name=task_name(name),
            payload=payload,
            key=key,
            queue=queue,
            conflict=conflict,
            max_attempts=max_attempts,
            priority=priority,
            metadata=metadata,
            delay_ms=delay_ms,
        )

    async def get(self, task_id: str) -> Task | None:
        return await self._store.get(task_id)

    async def get_by_key(self, key: str) -> Task | None:
        return await self._store.get_by_key(key)

    async def get_status(self, task_id: str) -> TaskRef | None:
        """The status-only probe wait polls on: id + status, no payload. Public
        for the same reason it exists — a dashboard or poller that only asks
        "is it finished yet" should not drag the payload back per ask."""
        return await self._store.get_status(task_id)

    async def get_status_by_key(self, key: str) -> TaskRef | None:
        return await self._store.get_status_by_key(key)

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

    async def purge(
        self,
        *,
        older_than_ms: int = 0,
        queue: str | None = None,
        status: TaskStatus | None = None,
        name: str | None = None,
        limit: int = 1_000,
    ) -> list[str]:
        """Delete terminal tasks that finished more than `older_than_ms` ago and
        return their ids. Nothing else in CairnQ removes rows, so a long-lived
        database needs this on a schedule — `retention` is this call on a
        timer. Each call is bounded by `limit` to keep the write short; loop
        until it returns fewer than `limit`.

        `queue` / `status` / `name` narrow the sweep — one installation carrying
        two workloads needs a retention per workload, not one for the whole
        database, and without these the shortest-lived tier sets the retention
        for every row."""
        return await self._store.purge(
            older_than_ms=older_than_ms, queue=queue, status=status, name=name, limit=limit
        )

    async def stats(self, queue: str | None = None) -> dict[str, dict[TaskStatus, int]]:
        """Task counts per queue, keyed by status and zero-filled across all
        statuses — `stats()["default"]["queued"]` is the backlog of a queue.
        `queue` narrows the aggregate to one queue, which is also what keeps a
        caller from paying for the other workloads sharing the installation; a
        named queue is always present, zero-filled if it has no rows.

        This counts rows, so it costs what it counts — use it for a dashboard,
        and poll `queue_depth()` instead, which is bounded."""
        return await self._store.stats(queue)

    async def queue_depth(self, queue: str, max_depth: int) -> int:
        """How many more tasks fit on `queue` under `max_depth` — 0 once it is
        full. The non-blocking read behind `max_queue_depth`, for a producer that
        would rather shed load or pick another queue than wait. Bounded at
        `max_depth` index entries, so it stays cheap to ask on every enqueue."""
        return await self._store.queue_depth(queue, max_depth)

    async def wait(
        self,
        task_id: str,
        *,
        timeout_ms: int = DEFAULT_WAIT_TIMEOUT_MS,
        poll_ms: int = DEFAULT_POLL_MS,
        max_poll_ms: int = MAX_POLL_MS,
    ) -> Task:
        """Wait for a task to finish. Returns the terminal Task (any status);
        raises TaskTimeout without stopping the task, so `wait(err.task_id)` picks
        the same wait back up — from another process, or after a longer deadline.

        `poll_ms` is the first poll interval; it backs off towards `max_poll_ms`,
        which is worth raising for a task known to take minutes (fewer reads) or
        lowering when completion-detection latency matters."""
        return await poll_wait(
            self._store,
            task_id,
            timeout_ms=timeout_ms,
            poll_ms=poll_ms,
            max_poll_ms=max_poll_ms,
        )

    async def wait_by_key(
        self,
        key: str,
        *,
        timeout_ms: int = DEFAULT_WAIT_TIMEOUT_MS,
        poll_ms: int = DEFAULT_POLL_MS,
        max_poll_ms: int = MAX_POLL_MS,
    ) -> Task:
        """Wait for whatever task the `key` currently points at — the
        cross-process form of picking a wait back up, when the id was never in
        hand or the process that held it is gone. Re-resolves the key on each
        poll, so a `replace` landing mid-wait moves the wait onto the new task,
        and a key with no task yet is waited for rather than rejected."""
        return await poll_wait_by_key(
            self._store, key, timeout_ms=timeout_ms, poll_ms=poll_ms, max_poll_ms=max_poll_ms
        )

    async def call(
        self,
        name: str | TaskDef[Any, Any],
        payload: dict[str, Any] | None = None,
        *,
        timeout_ms: int = DEFAULT_WAIT_TIMEOUT_MS,
        poll_ms: int = DEFAULT_POLL_MS,
        max_poll_ms: int = MAX_POLL_MS,
        **submit_kwargs: Any,
    ) -> Any:
        """submit + wait. Returns the result on success; raises TaskFailed /
        TaskCanceled / TaskTimeout otherwise. Accepts a name string or a TaskDef
        (its name is used).

        `timeout_ms` bounds the wait, not the task: on timeout the task runs
        on, and `wait(err.task_id)` — or `wait_by_key`, from a process that only
        has the key — resumes the wait rather than starting the work over."""
        task = await self.submit(name, payload, **submit_kwargs)
        final = await self.wait(
            task.id, timeout_ms=timeout_ms, poll_ms=poll_ms, max_poll_ms=max_poll_ms
        )
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
