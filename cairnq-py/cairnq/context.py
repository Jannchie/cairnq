from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from ._backoff import DEFAULT_RETRY_BACKOFF_MAX_MS, DEFAULT_RETRY_BACKOFF_MS, fail_delay_ms
from ._wait import DEFAULT_POLL_MS, poll_wait
from .errors import LostLease, as_envelope
from .models import Task, TaskDef, task_name
from .store.base import TaskStore


class TaskContext:
    """Handed to a task handler. Worker-side capabilities mirror the TS SDK.

    One of these per task, whether a handler is delivered one task or a batch: a
    batch handler receives a `list[TaskContext]`, so a single-task handler's `ctx`
    is literally the batch-of-one element. Lease, cancellation and settlement are
    per task, which is why they live here rather than on anything batch-shaped."""

    def __init__(
        self,
        store: TaskStore,
        task: Task,
        worker_id: str,
        lease_ms: int,
        *,
        retry_backoff_ms: int = DEFAULT_RETRY_BACKOFF_MS,
        retry_backoff_max_ms: int = DEFAULT_RETRY_BACKOFF_MAX_MS,
    ):
        self._store = store
        self._task = task
        self._worker_id = worker_id
        self._lease_ms = lease_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._retry_backoff_max_ms = retry_backoff_max_ms
        self._lease_lost = asyncio.Event()
        # Cancellation is monotonic: once the DB has told us a cancel was
        # requested it can't be taken back, so canceled() can answer from this
        # without a re-read.
        self._cancel_seen = False
        # Set once this task reached a terminal state through succeed()/fail().
        # The worker reads it to know which tasks a batch handler already
        # decided, so it neither settles them twice nor keeps renewing their
        # leases — the bookkeeping every ack/nack-style handler otherwise has to
        # carry itself.
        self._settled = False

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

    @property
    def settled(self) -> bool:
        """True once succeed() / fail() finalized this task. A batch handler can
        read it back, but it exists mainly so the worker knows which of a batch's
        tasks the handler already decided."""
        return self._settled

    @property
    def lost_lease(self) -> bool:
        """True once this worker has lost the task's lease — it expired and
        another worker reclaimed it. Nothing this handler writes will be recorded
        any more and the task is already running elsewhere, so a long handler
        should check this (or await `lease_lost`) and bail out instead of
        continuing to do side effects."""
        return self._lease_lost.is_set()

    @property
    def lease_lost(self) -> asyncio.Event:
        """Set when the lease is lost. Await it to race a handler against it."""
        return self._lease_lost

    def _mark_lease_lost(self) -> None:
        """Internal: called by the worker when an owned write reports lease loss."""
        self._lease_lost.set()

    def _mark_settled(self) -> None:
        """Internal: called by the worker when it finalizes this task itself, so
        `settled` means "this task is settled" rather than the narrower "the
        handler settled it" — the heartbeat and the settlement paths both read
        it, and both want the wider reading."""
        self._settled = True

    # Every owned write returns the current row, so cancellation and lease loss
    # ride along on writes the handler was making anyway.
    def _observe(self, task: Task) -> Task:
        self._observe_cancel(task.cancel_requested)
        return task

    def _observe_cancel(self, cancel_requested: bool) -> None:
        """The same observation from just the flag, for a caller that read it
        without materializing a Task — the shared heartbeat, whose statement
        returns only the id and the cancel column precisely so it does not have
        to drag every payload back on every beat."""
        if cancel_requested:
            self._cancel_seen = True

    def _require_lease(self) -> None:
        """The half of the gate that asks only "is this attempt still mine?".

        Checked locally, not just via the store's ownership check — after an
        abandoned (timed-out) attempt the same worker may re-claim this task
        under the same worker_id, and a zombie handler's write would then pass
        ownership against the NEW attempt.

        `_owned` layers the settled check on top for writes to this task;
        `submit` takes this half alone, because a handler may legitimately settle
        a task and then fan out from it."""
        if self._lease_lost.is_set():
            raise LostLease(self._task.id)

    async def _owned(self, write: Callable[[], Awaitable[Task]]) -> Task:
        # One gate for every write through this context, so "may I still write?"
        # is answered in one place rather than at each call site.
        #
        self._require_lease()
        # Settled: the task is terminal, so the statement would match no row and
        # come back as a lost lease — telling the handler "another worker took
        # this" when the truth is "you already finished it", and flipping
        # `lost_lease` on the way. Refuse here instead, without the round trip
        # and without corrupting the lease state.
        if self._settled:
            raise LostLease(self._task.id)
        try:
            return self._observe(await write())
        except LostLease:
            self._mark_lease_lost()
            raise

    async def progress(self, value: float | None, message: str | None = None) -> Task:
        return await self._owned(
            lambda: self._store.progress(
                task_id=self._task.id, worker_id=self._worker_id, progress=value, message=message
            )
        )

    async def heartbeat(self) -> Task:
        return await self._owned(
            lambda: self._store.heartbeat(
                task_id=self._task.id, worker_id=self._worker_id, lease_ms=self._lease_ms
            )
        )

    async def canceled(self) -> bool:
        """Cooperative cancel check. Free once a heartbeat has already seen the
        flag; otherwise reads the current row."""
        if self._cancel_seen:
            return True
        task = await self._store.get(self._task.id)
        if task is None:
            return True
        if task.cancel_requested:
            self._cancel_seen = True
        return self._cancel_seen or task.status == "canceled"

    # ------------------------------------------------------------- settlement
    # Finalizing a task is normally the worker's job, decided by whether the
    # handler returned or raised. These two let a handler decide one task itself,
    # which is what a batch needs: four of 256 tasks failing for four different
    # reasons is the ordinary case, not the edge one, and it cannot be expressed
    # by a single return value or a single raise.
    #
    # Settling twice is a no-op rather than an error. Handlers built on ack/nack
    # queues all end up carrying a `finalized_ids` set to guarantee exactly that;
    # holding it here instead is the point.

    async def succeed(self, result: Any = None) -> Task | None:
        """Finalize this task as succeeded, now, without waiting for the handler
        to return. `complete` semantics: a cancel requested while it ran wins and
        the task finalizes as canceled instead, its result discarded."""
        if self._settled:
            return None
        task = await self._owned(
            lambda: self._store.complete(
                task_id=self._task.id, worker_id=self._worker_id, result=result
            )
        )
        self._mark_settled()
        return task

    async def succeed_in(
        self, write: Callable[[Any], Awaitable[Any]]
    ) -> Task | None:
        """Finalize this task as succeeded, committing the caller's own writes in
        the SAME transaction as the settlement. Whatever `write` returns becomes
        the task's result.

            async def handler(ctx, payload):
                rendered = await render(payload)
                return await ctx.succeed_in(
                    lambda session: write_pages(session, rendered)
                )

        The alternative — write the rows, then settle — has a window between the
        two commits where the work is durable but the task still reads as
        running. A crash there re-runs the whole task, which for a render or an
        ingest means recomputing it, and for non-idempotent work means doing it
        twice.

        `session` is the driver's, so this needs a Postgres store built on a
        PgExecutor the application shares with its own driver; anything else
        raises NotImplementedError. If the settlement finds the lease gone,
        `write`'s work is rolled back with it and LostLease is raised. Returns
        None if this task was already settled.

        Mirrors `succeedIn` in the TypeScript SDK.
        """
        if self._settled:
            return None

        async def settle() -> Task:
            task, _ = await self._store.complete_in(
                task_id=self._task.id, worker_id=self._worker_id, write=write
            )
            return task

        task = await self._owned(settle)
        self._mark_settled()
        return task

    async def fail(
        self, error: Any = "task failed", *, retryable: bool = True
    ) -> Task | None:
        """Finalize this task as failed, now. `error` may be a string reason, an
        exception, a TaskError (which carries its own retryability), or a ready
        envelope. Retryable failures get the worker's backoff and are re-queued
        while attempts remain, exactly as a raised exception would be."""
        if self._settled:
            return None
        envelope, retryable = as_envelope(error, retryable)
        task = await self._owned(
            lambda: self._store.fail(
                task_id=self._task.id,
                worker_id=self._worker_id,
                error=envelope,
                retryable=retryable,
                delay_ms=fail_delay_ms(
                    self._task.attempt,
                    retryable=retryable,
                    base_ms=self._retry_backoff_ms,
                    max_ms=self._retry_backoff_max_ms,
                ),
            )
        )
        self._mark_settled()
        return task

    async def submit(
        self, name: str | TaskDef[Any, Any], payload: dict[str, Any] | None = None, **kwargs: Any
    ) -> Task:
        """Submit a child task. parent/root/correlation are wired automatically
        so the whole chain is queryable via list(root_id=...).

        Refused once the lease is gone. Not the full `_owned` gate — a handler
        may legitimately settle a task and then fan out from it, so `settled` is
        no bar — but a context whose lease was lost is an attempt that has been
        abandoned: it is being retried elsewhere, and every child it creates now
        will be created again by that retry. Creating work is the one side effect
        cairnq can actually stop a zombie handler from repeating."""
        self._require_lease()
        return await self._store.submit(
            name=task_name(name),
            payload=payload,
            parent_id=self._task.id,
            root_id=self._task.root_id,
            correlation_id=self._task.correlation_id,
            **kwargs,
        )

    async def wait(
        self, task_id: str, *, timeout_ms: int = 30_000, poll_ms: int = DEFAULT_POLL_MS
    ) -> Task:
        return await poll_wait(self._store, task_id, timeout_ms=timeout_ms, poll_ms=poll_ms)
