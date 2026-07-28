from __future__ import annotations

import asyncio
import contextlib
import inspect
import signal
from typing import Any, Callable, Literal

from ._ids import new_id
from .context import TaskContext
from .errors import LostLease, TaskError, error_envelope
from .models import Task, TaskDef, task_name
from .store.base import TaskStore
from .store.sqlite import SQLiteStore

Handler = Callable[..., Any]
# Where an error the worker recovered from came from.
ErrorPhase = Literal["claim", "execute"]
OnError = Callable[[BaseException, dict[str, Any]], None]

DEFAULT_RETRY_BACKOFF_MS = 1_000
DEFAULT_RETRY_BACKOFF_MAX_MS = 30_000
# Wait after a failed claim, so a broken database is not polled in a tight loop.
CLAIM_ERROR_BACKOFF_S = 0.25


def retry_delay_ms(attempt: int, *, base_ms: int, max_ms: int) -> int:
    """Exponential backoff for the next attempt of a task that just failed."""
    if base_ms <= 0:
        return 0
    return min(max_ms, base_ms * 2 ** max(0, attempt - 1))


def _exception_envelope(exc: BaseException) -> dict[str, Any]:
    return error_envelope(
        type=type(exc).__name__, code="handler_error", message=str(exc), retryable=True
    )


def _wants_payload(handler: Handler) -> bool:
    """Whether a handler declares a parameter to receive the payload — a second
    positional arg, or *args. Decided once from the static signature at
    registration time, never re-derived per task run."""
    P = inspect.Parameter
    params = list(inspect.signature(handler).parameters.values())
    positional = [p for p in params if p.kind in (P.POSITIONAL_ONLY, P.POSITIONAL_OR_KEYWORD)]
    has_var_positional = any(p.kind is P.VAR_POSITIONAL for p in params)
    # ctx fills the first positional slot; payload fills the second if present.
    return len(positional) >= 2 or has_var_positional


def _required_name(fn: Handler) -> str:
    """Task name derived from a handler registered without an explicit one. Rejects
    a name that can't serve as a task name (e.g. a lambda's '<lambda>'), mirroring
    the TS SDK's guard against anonymous handlers."""
    name = getattr(fn, "__name__", "")
    if not name.isidentifier():
        raise ValueError(
            f"worker.task: cannot derive a task name from {name or fn!r}; pass one "
            "explicitly, e.g. @worker.task('summary.create')"
        )
    return name


async def _invoke(
    handler: Handler, wants_payload: bool, ctx: TaskContext, payload: dict[str, Any]
) -> Any:
    """One convention, shared with the TS SDK: a handler receives `(ctx, payload)`,
    where `payload` is the whole dict — destructure it yourself. A handler that
    declares only `ctx` is called with `ctx` alone (`wants_payload` is False)."""
    result = handler(ctx, payload) if wants_payload else handler(ctx)
    if inspect.isawaitable(result):
        result = await result
    return result


class Worker:
    def __init__(
        self,
        store: TaskStore,
        queues: list[str] | tuple[str, ...],
        *,
        concurrency: int = 1,
        lease_ms: int = 30_000,
        heartbeat_interval_ms: int | None = None,
        poll_interval_ms: int = 500,
        claim_batch: int | None = None,
        retry_backoff_ms: int = DEFAULT_RETRY_BACKOFF_MS,
        retry_backoff_max_ms: int = DEFAULT_RETRY_BACKOFF_MAX_MS,
        on_error: OnError | None = None,
    ):
        self._store = store
        self._queues = list(queues)
        self._concurrency = concurrency
        self._lease_ms = lease_ms
        self._hb_interval = heartbeat_interval_ms or max(1_000, lease_ms // 3)
        self._poll = poll_interval_ms
        self._batch = claim_batch
        self._retry_backoff_ms = retry_backoff_ms
        self._retry_backoff_max_ms = retry_backoff_max_ms
        # Called for errors the worker survived — a claim that threw, a store
        # write that failed while finalizing a task. Without it these are silent:
        # the run loop carries on either way, so this is the only place an
        # operator learns a worker is limping.
        self._on_error = on_error
        # name -> (handler, wants_payload); the payload-arity decision is cached
        # here at registration so the worker hot path never re-inspects signatures.
        self._handlers: dict[str, tuple[Handler, bool]] = {}
        self._worker_id = new_id("worker")
        self._stop = asyncio.Event()
        # True only when this worker created its own store (via .sqlite); an
        # injected store may be shared, so serve()/background() must not close it.
        self._owns_store = False

    @classmethod
    def sqlite(
        cls,
        path: str,
        *,
        queues: list[str] | tuple[str, ...] = ("default",),
        concurrency: int = 1,
        lease_ms: int = 30_000,
        busy_timeout_ms: int = 5_000,
        **kwargs: Any,
    ) -> "Worker":
        worker = cls(
            SQLiteStore(path, busy_timeout_ms=busy_timeout_ms),
            queues,
            concurrency=concurrency,
            lease_ms=lease_ms,
            **kwargs,
        )
        worker._owns_store = True
        return worker

    @classmethod
    def postgres(
        cls,
        dsn: str,
        *,
        queues: list[str] | tuple[str, ...] = ("default",),
        concurrency: int = 1,
        lease_ms: int = 30_000,
        min_size: int = 1,
        max_size: int = 10,
        **kwargs: Any,
    ) -> "Worker":
        """Multi-host backend. `dsn` is a libpq connection string; requires the
        optional asyncpg package (install cairnq[postgres])."""
        from .store.postgres import PostgresStore

        worker = cls(
            PostgresStore(dsn, min_size=min_size, max_size=max_size),
            queues,
            concurrency=concurrency,
            lease_ms=lease_ms,
            **kwargs,
        )
        worker._owns_store = True
        return worker

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def task(self, name: str | TaskDef[Any, Any] | Handler | None = None) -> Any:
        """Register a handler. Usable bare (`@worker.task` — registered under the
        function's name), with an explicit name (`@worker.task("summary.create")`)
        for dotted/namespaced or cross-language matching, or with a TaskDef
        (`@worker.task(my_task)`) so the name is shared with the submit side."""
        if callable(name):  # used bare: @worker.task
            self.register(_required_name(name), name)
            return name

        resolved = None if name is None else task_name(name)

        def decorator(fn: Handler) -> Handler:
            self.register(resolved or _required_name(fn), fn)
            return fn

        return decorator

    def register(self, name: str, fn: Handler) -> None:
        self._handlers[name] = (fn, _wants_payload(fn))

    def stop(self) -> None:
        self._stop.set()

    async def close(self) -> None:
        """Close the underlying store connection. Call after run() returns for a
        clean shutdown (otherwise the aiosqlite background thread may outlive the
        event loop)."""
        await self._store.close()

    async def _close_if_owned(self) -> None:
        # serve()/background() only close a store the worker created itself (via
        # Worker.sqlite). An injected store may be shared with a CairnQ client, so
        # closing it here would pull the connection out from under it.
        if self._owns_store:
            with contextlib.suppress(BaseException):
                await self.close()

    def serve(self, *, concurrency: int | None = None) -> None:
        """Blocking entry point for a standalone worker process: run until
        SIGINT/SIGTERM, then close the store cleanly. Call it from a script's top
        level — no `asyncio.run` boilerplate. Use `run()` / `background()` instead
        when you already have a running event loop."""

        async def _main() -> None:
            try:
                await self.run(concurrency=concurrency)
            finally:
                await self._close_if_owned()

        asyncio.run(_main())

    def _report(self, exc: BaseException, **info: Any) -> None:
        if self._on_error is None:
            return
        with contextlib.suppress(BaseException):
            # A reporting hook must never take the worker down with it.
            self._on_error(exc, info)

    # ----------------------------------------------------------------- run loop
    async def run(self, *, concurrency: int | None = None) -> None:
        if concurrency:
            self._concurrency = concurrency
        batch = self._batch or self._concurrency
        await self._store.connect()
        self._install_signal_handlers()
        running: set[asyncio.Task] = set()
        try:
            while not self._stop.is_set():
                free = self._concurrency - len(running)
                if free <= 0:
                    # Wait for a slot rather than polling for one. _execute never
                    # raises, so waiting on it cannot surface an exception here.
                    await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                    continue
                try:
                    claimed = await self._store.claim(
                        queues=self._queues,
                        # Only what this worker can run. Queues do not partition
                        # work by task name, so another worker's tasks would
                        # otherwise be claimed here and failed for want of a
                        # handler. Read each poll: handlers may be registered
                        # after run() started.
                        names=list(self._handlers),
                        worker_id=self._worker_id,
                        lease_ms=self._lease_ms,
                        limit=min(batch, free),
                    )
                except Exception as exc:
                    # A claim can fail transiently (lock contention, a dropped
                    # connection). Report it and keep polling — one bad poll must
                    # not end the worker.
                    self._report(exc, phase="claim")
                    await self._sleep_or_stop(CLAIM_ERROR_BACKOFF_S)
                    continue
                if not claimed:
                    await self._sleep_or_stop(self._poll / 1000)
                    continue
                for task in claimed:
                    fut = asyncio.create_task(self._execute(task))
                    running.add(fut)
                    fut.add_done_callback(running.discard)
        finally:
            if running:
                await asyncio.gather(*running, return_exceptions=True)

    async def _execute(self, task: Task) -> None:
        """Run one task to completion. Never raises: a task-level failure is
        reported through on_error and the loop moves on."""
        ctx = TaskContext(self._store, task, self._worker_id, self._lease_ms)
        hb = asyncio.create_task(self._heartbeat_loop(ctx))
        try:
            entry = self._handlers.get(task.name)
            if entry is None:
                await self._safe_fail(
                    task,
                    error_envelope(
                        type="NoHandler",
                        code="no_handler",
                        message=f"no handler registered for {task.name!r}",
                        retryable=False,
                    ),
                    retryable=False,
                )
                return
            handler, wants_payload = entry
            try:
                result = await _invoke(handler, wants_payload, ctx, task.payload)
            except LostLease:
                return
            except TaskError as exc:  # handler chose how to fail
                await self._safe_fail(task, exc.envelope(), retryable=exc.retryable)
                return
            except Exception as exc:  # any other handler error is retryable
                await self._safe_fail(task, _exception_envelope(exc), retryable=True)
                return
            try:
                # complete (not succeed): finalizes as canceled if a cancel was
                # requested while the handler ran, else succeeded.
                await self._store.complete(
                    task_id=task.id, worker_id=self._worker_id, result=result
                )
            except LostLease:
                ctx._mark_lease_lost()
                return
        except Exception as exc:
            self._report(exc, phase="execute", task_id=task.id)
        finally:
            hb.cancel()
            with contextlib.suppress(BaseException):
                await hb

    async def _heartbeat_loop(self, ctx: TaskContext) -> None:
        try:
            while True:
                await asyncio.sleep(self._hb_interval / 1000)
                try:
                    await ctx.heartbeat()
                except LostLease:
                    # ctx.heartbeat() already flagged the lease for the handler.
                    return
                except Exception as exc:
                    self._report(exc, phase="execute", task_id=ctx.task_id)
        except asyncio.CancelledError:
            return

    async def _safe_fail(self, task: Task, envelope: dict[str, Any], *, retryable: bool) -> None:
        delay_ms = (
            retry_delay_ms(
                task.attempt,
                base_ms=self._retry_backoff_ms,
                max_ms=self._retry_backoff_max_ms,
            )
            if retryable
            else 0
        )
        try:
            await self._store.fail(
                task_id=task.id, worker_id=self._worker_id, error=envelope,
                retryable=retryable, delay_ms=delay_ms,
            )
        except LostLease:
            pass

    async def _sleep_or_stop(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    def _install_signal_handlers(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            pass  # not the main thread / unsupported platform

    @contextlib.asynccontextmanager
    async def background(self, *, concurrency: int | None = None):
        """Run the worker in the same process (deployment mode A)."""
        runner = asyncio.create_task(self.run(concurrency=concurrency))
        try:
            yield self
        finally:
            self.stop()
            with contextlib.suppress(BaseException):
                await runner
            await self._close_if_owned()
