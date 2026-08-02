from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import signal
from typing import Any, Callable, Literal

from ._ids import new_id
from .context import TaskContext
from .errors import LostLease, SerializationError, TaskError, error_envelope
from .models import Task, TaskDef, task_name
from .store.base import TaskStore
from .store.postgres import PostgresStore
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


class _AttemptTimeout(Exception):
    """Internal: an attempt outran max_run_ms and was abandoned."""


def _timeout_envelope(name: str, max_run_ms: int) -> dict[str, Any]:
    return error_envelope(
        type="HandlerTimeout",
        code="handler_timeout",
        message=f"handler for {name!r} exceeded max_run_ms={max_run_ms}ms; "
        "the attempt was abandoned",
        retryable=True,
    )


def _payload_bytes(task: Task) -> int:
    """Resident size of a task's payload, for the max_in_flight_bytes budget.

    Measured by re-serializing rather than read off the row: a jsonb-aware driver
    (asyncpg) hands back an already-decoded object with no wire length attached,
    so there is nothing to read there. What the budget is really after is the
    memory a payload pins while its handler runs, and its JSON length tracks that
    closely enough to size one by."""
    try:
        return len(json.dumps(task.payload, ensure_ascii=False).encode())
    except (TypeError, ValueError):
        # Unmeasurable, and it came out of the store, so it is already resident:
        # charging nothing under-counts, but failing the claim over an accounting
        # detail would drop a task the worker can otherwise run.
        return 0


def _consume_result(task: asyncio.Task) -> None:
    """Retrieve an abandoned attempt's outcome so asyncio never logs it as an
    unretrieved exception. The outcome itself is discarded — the task was
    already failed as handler_timeout."""
    if not task.cancelled():
        task.exception()


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
        max_run_ms: int | None = None,
        max_in_flight_bytes: int | None = None,
        on_error: OnError | None = None,
    ):
        if max_run_ms is not None and max_run_ms <= 0:
            raise ValueError(f"max_run_ms must be > 0, got {max_run_ms}")
        if max_in_flight_bytes is not None and max_in_flight_bytes <= 0:
            raise ValueError(f"max_in_flight_bytes must be > 0, got {max_in_flight_bytes}")
        self._store = store
        self._queues = list(queues)
        self._concurrency = concurrency
        self._lease_ms = lease_ms
        # lease/3 gives two beats of slack; the floor only matters for sub-150ms leases.
        self._hb_interval = heartbeat_interval_ms or max(50, lease_ms // 3)
        self._poll = poll_interval_ms
        self._batch = claim_batch
        self._retry_backoff_ms = retry_backoff_ms
        self._retry_backoff_max_ms = retry_backoff_max_ms
        # Wall-clock ceiling for one attempt. The heartbeat renews the lease for
        # as long as a handler runs, so a hung handler would otherwise hold its
        # task `running` (and its concurrency slot) forever — cancel can't help,
        # cooperative checks need a live handler. None disables the ceiling.
        self._max_run_ms = max_run_ms
        # Resident payload bytes allowed across running handlers, independent of
        # their count. `concurrency` bounds tasks, not memory, so a worker sized
        # for small payloads holds concurrency * largest-payload bytes the moment
        # a batch of big ones arrives — for payloads carrying media inline, the
        # difference between megabytes and gigabytes resident. Once spent, the
        # worker stops claiming until running handlers give it back.
        #
        # The bound is on tasks already executing: a claim commits to a whole
        # batch before any size is known, so one batch can overshoot by up to
        # `claim_batch` payloads (lower it to tighten that), and a single payload
        # larger than the whole budget still runs — alone, rather than
        # deadlocking the worker. None disables the budget, and the measurement
        # with it.
        self._max_in_flight_bytes = max_in_flight_bytes
        self._in_flight_bytes = 0
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
            # Signals are installed here rather than in run(): serve() is the entry
            # point that owns the process. run()/background() embed the worker in
            # someone else's process, where taking SIGINT/SIGTERM replaces the
            # host's own shutdown handler for good.
            remove_signal_handlers = self._install_signal_handlers()
            try:
                await self.run(concurrency=concurrency)
            finally:
                remove_signal_handlers()
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
        # A per-run override stays local, as in the TS SDK — it must not stick to
        # the worker and silently apply to the next run(). Clamped: at 0 the
        # loop would await asyncio.wait(set()), which raises.
        concurrency = max(1, concurrency or self._concurrency)
        batch = self._batch or concurrency
        await self._store.connect()
        running: set[asyncio.Task] = set()
        try:
            while not self._stop.is_set():
                free = concurrency - len(running)
                # Two ceilings, either of which stops the claim: task count and
                # resident payload bytes. The byte arm is guarded on `running`
                # being non-empty because it must never be the reason we wait on
                # an empty set — asyncio.wait(set()) raises. With nothing
                # running, nothing is resident, so the budget cannot be what is
                # holding us back anyway.
                over_budget = (
                    self._max_in_flight_bytes is not None
                    and self._in_flight_bytes >= self._max_in_flight_bytes
                )
                if running and (free <= 0 or over_budget):
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
                    await self._idle(self._poll)
                    continue
                for task in claimed:
                    # Charged before the handler starts and refunded when it
                    # settles, so the budget covers exactly the span the payload
                    # is pinned in memory.
                    charged = 0 if self._max_in_flight_bytes is None else _payload_bytes(task)
                    self._in_flight_bytes += charged
                    fut = asyncio.create_task(self._execute(task))
                    running.add(fut)
                    fut.add_done_callback(running.discard)
                    fut.add_done_callback(
                        lambda _f, size=charged: self._release_bytes(size)
                    )
        finally:
            if running:
                await asyncio.gather(*running, return_exceptions=True)

    def _release_bytes(self, size: int) -> None:
        """Refund a finished handler's payload charge. Clamped at zero: the
        budget is an accounting aid, and a running total that drifted negative
        would silently widen it for every task after."""
        self._in_flight_bytes = max(0, self._in_flight_bytes - size)

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
                result = await self._attempt(handler, wants_payload, ctx, task)
            except LostLease:
                return
            except _AttemptTimeout:
                # Recorded as a retryable failure, so backoff / max_attempts /
                # cancel-wins all apply exactly as for a raised exception.
                await self._safe_fail(
                    task, _timeout_envelope(task.name, self._max_run_ms), retryable=True
                )
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
            except SerializationError as exc:
                # The handler succeeded but its return value can't cross the JSON
                # protocol. Deterministic, so fail fast and permanently — the
                # alternative is sitting `running` until lease expiry redelivers
                # a task that fails the same way every attempt.
                await self._safe_fail(
                    task,
                    error_envelope(
                        type="SerializationError",
                        code="unserializable_result",
                        message=f"handler result is not JSON-serializable: {exc}",
                        retryable=False,
                    ),
                    retryable=False,
                )
                return
        except Exception as exc:
            self._report(exc, phase="execute", task_id=task.id)
        finally:
            hb.cancel()
            with contextlib.suppress(BaseException):
                await hb

    async def _attempt(
        self, handler: Handler, wants_payload: bool, ctx: TaskContext, task: Task
    ) -> Any:
        """Run one attempt, bounded by max_run_ms when set. On timeout the
        attempt is abandoned: the context is flagged lease-lost first (so a
        handler that survives cancellation can never write again — see
        TaskContext._owned), then the handler task is cancelled and left to
        die at its next await. The caller records the handler_timeout failure;
        lease recovery is NOT involved, so redelivery is immediate."""
        if self._max_run_ms is None:
            return await _invoke(handler, wants_payload, ctx, task.payload)
        runner = asyncio.create_task(_invoke(handler, wants_payload, ctx, task.payload))
        done, _ = await asyncio.wait({runner}, timeout=self._max_run_ms / 1000)
        if done:
            return runner.result()  # or re-raises what the handler raised
        ctx._mark_lease_lost()
        runner.cancel()
        # Not awaited: a handler that shields itself from cancellation would
        # otherwise hold this slot for exactly the hang the ceiling exists for.
        runner.add_done_callback(_consume_result)
        raise _AttemptTimeout

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

    async def _idle(self, poll_ms: int) -> None:
        """The empty-poll sleep. A store with a push channel (Postgres
        LISTEN/NOTIFY) cuts it short when a task on this worker's queues
        becomes claimable; stop() interrupts it either way. Both sides bound
        themselves at the poll interval, so the poll fallback — which also
        drives lease recovery — never stretches."""
        racers = {
            asyncio.create_task(self._store.claim_wake(self._queues, poll_ms)),
            asyncio.create_task(self._sleep_or_stop(poll_ms / 1000)),
        }
        _, pending = await asyncio.wait(racers, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
            with contextlib.suppress(BaseException):
                await t

    async def _sleep_or_stop(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    def _install_signal_handlers(self) -> Callable[[], None]:
        """Take SIGINT/SIGTERM for the duration of serve(). Returns the undo."""
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []

        def remove() -> None:
            for sig in installed:
                with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                    loop.remove_signal_handler(sig)
            installed.clear()

        def handle() -> None:
            # Stand down after the first signal, so a second Ctrl-C reaches
            # Python's default and interrupts a worker that will not drain.
            remove()
            self._stop.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, handle)
                installed.append(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass  # not the main thread / unsupported platform
        return remove

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
