from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import signal
from collections import defaultdict
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Callable, Literal

from ._backoff import (
    DEFAULT_RETRY_BACKOFF_MAX_MS,
    DEFAULT_RETRY_BACKOFF_MS,
    fail_delay_ms,
    retry_delay_ms,
)
from ._ids import new_id
from .backpressure import (
    DEFAULT_MAX_WAIT_MS,
    INITIAL_PROBE_INTERVAL_MS,
    QueueDepthLimit,
)
from .context import TaskContext
from .errors import LostLease, SerializationError, as_envelope, error_envelope
from .models import Task, TaskDef, task_name
from .store.base import TaskStore
from .store.postgres import PostgresStore
from .store.sqlite import SQLiteStore

Handler = Callable[..., Any]
# Where an error the worker recovered from came from.
ErrorPhase = Literal["claim", "execute"]
OnError = Callable[[BaseException, dict[str, Any]], None]

# retry_delay_ms and the two backoff defaults are imported above rather than
# defined here: they moved to _backoff so TaskContext.fail could share them
# without context.py importing the module that imports it. They stay importable
# from this module, which is where they used to live.

# Wait after a failed claim, so a broken database is not polled in a tight loop.
CLAIM_ERROR_BACKOFF_S = 0.25


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


# Module-level for the same reason store/base.py keeps one: json.dumps builds a
# fresh JSONEncoder for any non-default kwarg, which on the claim path would be
# per-task construction for nothing. Separators match dump_json so a payload
# measures the same as it was stored.
_SIZE_ENCODER = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"))


def _payload_bytes(task: Task) -> int:
    """Resident size of a task's payload, for the max_in_flight_bytes budget.

    Re-serializes because by this point the wire form is gone. Both Python
    backends do hand one back — asyncpg registers no jsonb codec, so
    `Task.from_row` holds the serialized str right up until json.loads discards
    it — and capturing its length there would make this free, at the cost of
    carrying a non-protocol field on Task in both SDKs. Left for when this shows
    up in a profile.

    What the budget is really after is the memory a payload pins while its
    handler runs, and its JSON length tracks that closely enough to size one by."""
    try:
        return len(_SIZE_ENCODER.encode(task.payload).encode())
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


async def _call(handler: Handler, *args: Any) -> Any:
    """Call a handler and await it if it returned an awaitable — the one place
    that decides sync handlers are as welcome as async ones.

    The argument list is the calling convention, and there are three: a
    single-task handler receives `(ctx, payload)` (the whole payload dict —
    destructure it yourself), or `(ctx)` alone when it declares no payload
    parameter, and a batch handler receives one argument, the list of contexts.
    There is no payload shortcut to pair with the batch form: payloads are per
    task, so they are read off the items (`item.payload`), which is also what a
    handler must hold to settle one of them."""
    result = handler(*args)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class _Registration:
    """What `worker.task` recorded for one task name."""

    fn: Handler
    #: Whether a single-task handler declared a payload parameter, decided once
    #: from the signature so the hot path never re-inspects it.
    wants_payload: bool
    #: Tasks per handler call, or None for one-at-a-time delivery.
    batch: int | None = None


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
        # Backpressure is accepted here too, not only on CairnQ: a handler
        # spawning children through TaskContext.submit is a producer, and in a
        # worker process there is usually no CairnQ handle to have configured
        # the store.
        max_queue_depth: QueueDepthLimit | None = None,
        max_queue_wait_ms: int = DEFAULT_MAX_WAIT_MS,
        queue_poll_interval_ms: int = INITIAL_PROBE_INTERVAL_MS,
        on_error: OnError | None = None,
    ):
        if max_run_ms is not None and max_run_ms <= 0:
            raise ValueError(f"max_run_ms must be > 0, got {max_run_ms}")
        if max_in_flight_bytes is not None and max_in_flight_bytes <= 0:
            raise ValueError(f"max_in_flight_bytes must be > 0, got {max_in_flight_bytes}")
        if max_queue_depth is not None:
            store.use_backpressure(
                max_queue_depth,
                max_queue_wait_ms=max_queue_wait_ms,
                queue_poll_interval_ms=queue_poll_interval_ms,
            )
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
        # Tasks charged to running handler calls. A counter rather than a sum
        # over `running` each turn: the loop re-reads it on every poll tick *and*
        # every time a slot frees while saturated, and it is charged and refunded
        # in exactly the same places as _in_flight_bytes.
        self._in_flight_tasks = 0
        # Called for errors the worker survived — a claim that threw, a store
        # write that failed while finalizing a task. Without it these are silent:
        # the run loop carries on either way, so this is the only place an
        # operator learns a worker is limping.
        self._on_error = on_error
        # name -> registration; the payload-arity decision and the batch size are
        # settled here at registration so the worker hot path never re-inspects
        # signatures.
        self._handlers: dict[str, _Registration] = {}
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

    def task(
        self, name: str | TaskDef[Any, Any] | Handler | None = None, *, batch: int | None = None
    ) -> Any:
        """Register a handler. Usable bare (`@worker.task` — registered under the
        function's name), with an explicit name (`@worker.task("summary.create")`)
        for dotted/namespaced or cross-language matching, or with a TaskDef
        (`@worker.task(my_task)`) so the name is shared with the submit side.

        `batch=N` switches this name to batch delivery: the handler takes one
        argument, a `list[TaskContext]` of up to N tasks, instead of
        `(ctx, payload)`. Use it when the work itself is batched — one embedding
        call over 256 texts rather than 256 calls — and size N by what the
        downstream API wants, not by the queue."""
        if callable(name):  # used bare: @worker.task
            self.register(_required_name(name), name, batch=batch)
            return name

        resolved = None if name is None else task_name(name)

        def decorator(fn: Handler) -> Handler:
            self.register(resolved or _required_name(fn), fn, batch=batch)
            return fn

        return decorator

    def register(self, name: str, fn: Handler, *, batch: int | None = None) -> None:
        if batch is not None and batch < 1:
            raise ValueError(f"batch must be >= 1, got {batch}")
        self._handlers[name] = _Registration(fn, _wants_payload(fn), batch)

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
        await self._store.connect()
        # The calls in flight. How many *tasks* they hold is counted separately,
        # in _in_flight_tasks: `concurrency` bounds tasks, not calls, so a batch
        # call holding 8 tasks holds 8 of the budget and a worker cannot
        # accumulate batches until it is running far more tasks than it was
        # configured for. It also makes `concurrency` the ceiling on batch size,
        # since a claim can never exceed the free budget.
        running: set[asyncio.Task] = set()
        claim_ceiling = self._batch or concurrency
        try:
            while not self._stop.is_set():
                free = concurrency - self._in_flight_tasks
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
                    # Wait for a slot rather than polling for one. _run_call never
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
                        # `concurrency` bounds tasks in flight, batched or not, so
                        # a claim never exceeds it. Scaling this by the widest
                        # registered batch would let one poll return `concurrency
                        # * batch` rows — and a claim is filtered by queue and
                        # name set, not by delivery mode, so a queue holding
                        # unbatched work would turn every one of those rows into
                        # its own handler call. One `batch=256` registration would
                        # then run 256 unrelated handlers on a worker configured
                        # for one. So the batch a handler sees is bounded by
                        # `concurrency` too: size it for the batch you want.
                        limit=min(claim_ceiling, free),
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
                for reg, group in self._deliveries(claimed):
                    # Charged before the handler starts and refunded when it
                    # settles, so the budgets cover exactly the span the tasks are
                    # held and their payloads pinned in memory.
                    charged = (
                        0
                        if self._max_in_flight_bytes is None
                        else sum(_payload_bytes(t) for t in group)
                    )
                    self._in_flight_bytes += charged
                    self._in_flight_tasks += len(group)

                    def settled(
                        fut: asyncio.Task, size: int = charged, count: int = len(group)
                    ) -> None:
                        running.discard(fut)
                        self._in_flight_bytes -= size
                        self._in_flight_tasks -= count

                    # An unregistered name has nothing to run, so it never starts
                    # a handler or a heartbeat; everything else is one call,
                    # batched or not.
                    coro = (
                        self._fail_no_handler(group[0])
                        if reg is None
                        else self._run_call(reg, group)
                    )
                    fut = asyncio.create_task(coro)
                    running.add(fut)
                    fut.add_done_callback(settled)
        finally:
            if running:
                await asyncio.gather(*running, return_exceptions=True)

    def _deliveries(
        self, claimed: list[Task]
    ) -> list[tuple[_Registration | None, list[Task]]]:
        """Split one claim into handler calls, each with the registration to run it.

        A claim is filtered by queue and by the names this worker handles, so it
        comes back mixed; batch size is per name (one embedding call wants 256
        texts, one Docling parse wants exactly 1). So group by name, then chunk
        each group by that name's size. Names registered without `batch` come back
        as one-task calls, as do names not registered at all — reachable only if a
        handler is unregistered mid-run, and dispatched to `_fail_no_handler`.

        The registration rides along because this is where it was resolved;
        looking it up again at the call site would put "is this name batched" in
        two places.
        """
        by_name: dict[str, list[Task]] = defaultdict(list)
        for task in claimed:
            by_name[task.name].append(task)
        out: list[tuple[_Registration | None, list[Task]]] = []
        for name, group in by_name.items():
            reg = self._handlers.get(name)
            size = (reg.batch if reg else None) or 1
            out.extend((reg, group[i : i + size]) for i in range(0, len(group), size))
        return out

    def _context(self, task: Task) -> TaskContext:
        return TaskContext(
            self._store,
            task,
            self._worker_id,
            self._lease_ms,
            retry_backoff_ms=self._retry_backoff_ms,
            retry_backoff_max_ms=self._retry_backoff_max_ms,
        )

    async def _fail_no_handler(self, task: Task) -> None:
        """Record a claimed task this worker cannot run. Reachable only if a name
        is unregistered mid-run — the claim filters on the registered names — so
        it does not start a handler or a heartbeat for a task it will not run."""
        with contextlib.suppress(Exception):
            await self._safe_fail(
                self._context(task),
                error_envelope(
                    type="NoHandler",
                    code="no_handler",
                    message=f"no handler registered for {task.name!r}",
                    retryable=False,
                ),
                retryable=False,
            )

    async def _run_call(self, reg: _Registration, tasks: list[Task]) -> None:
        """Run one handler call to completion — one task, or a whole batch. Never
        raises: a task-level failure is reported through on_error and the loop
        moves on.

        One lifecycle for both delivery modes, because a single-task handler *is*
        the one-element case: the same heartbeat covers the call, the same
        classifier reads its exception, and the same rule settles what is left.
        Only two things vary — how the handler is called, and where a leftover
        task's result comes from — so those are the only two branches below.

        The contract is single: **when the handler returns, every task it did not
        settle itself is settled by how the call ended.** Returning succeeds them,
        raising fails them (retryably, or as the raised TaskError says). That is
        what keeps the ordinary cases free of bookkeeping — a handler that just
        returns has finished 256 tasks — while still letting it pick individual
        tasks off with `item.succeed()` / `item.fail()` as it goes.

        For a batch, a returned mapping of task id -> result fills in results for
        the tasks left over; anything else returned is ignored, and unmentioned
        tasks succeed with no result (the common shape, where the handler's output
        went to a database rather than into the task row). For a single task the
        return value simply is the result.
        """
        ctxs = [self._context(t) for t in tasks]
        batched = reg.batch is not None
        if batched:
            args: tuple[Any, ...] = (ctxs,)
        else:
            args = (ctxs[0], tasks[0].payload) if reg.wants_payload else (ctxs[0],)
        hb = asyncio.create_task(self._heartbeat_loop(ctxs))
        try:
            try:
                result = await self._attempt(lambda: _call(reg.fn, *args), ctxs)
            except Exception as exc:
                outcome = self._outcome_of(exc, tasks[0].name)
                if outcome is not None:
                    await self._settle_each(
                        ctxs, lambda c: self._safe_fail(c, outcome[0], retryable=outcome[1])
                    )
                return
            # A batch handler's return maps task id -> result; a single-task
            # handler's return *is* the result. Anything else a batch returns is
            # ignored, so its leftovers succeed with no result — hence the empty
            # mapping rather than falling through to `result`.
            results = (result if isinstance(result, dict) else {}) if batched else None
            await self._settle_each(
                ctxs,
                lambda c: self._succeed_one(
                    c, results.get(c.task_id) if results is not None else result
                ),
            )
        except Exception as exc:
            self._report(exc, phase="execute", task_id=tasks[0].id)
        finally:
            hb.cancel()
            with contextlib.suppress(BaseException):
                await hb

    def _outcome_of(
        self, exc: BaseException, name: str
    ) -> tuple[dict[str, Any], bool] | None:
        """How an attempt that ended badly is recorded: (envelope, retryable), or
        None when there is nothing to record.

        One classifier for both delivery modes, so a handler error cannot mean
        different things depending on how its task happened to be delivered. That
        includes `LostLease`, which is not an outcome at all: it means a write
        through this context was already rejected, so recording anything more
        would be rejected too. Both modes then leave the task alone — the
        single-task one has nothing else to do, and a batch lets its remaining
        tasks fall to lease expiry and redelivery rather than stamping them with
        a failure the handler never reported."""
        if isinstance(exc, LostLease):
            return None
        if isinstance(exc, _AttemptTimeout):
            # Retryable, so backoff / max_attempts / cancel-wins all apply
            # exactly as for a raised exception.
            return _timeout_envelope(name, self._max_run_ms), True
        # Everything else is what a handler could equally have passed to
        # ctx.fail(), so it goes through the same normalizer — a TaskError keeps
        # its own retryability, anything else is retryable.
        return as_envelope(exc, True)

    # Both settlement paths write through the store rather than through the
    # context. `TaskContext._owned` short-circuits once a lease is known lost,
    # which is there to stop a *zombie handler* writing after its attempt was
    # abandoned — but these run after the handler is done, on the worker's own
    # authority. Ownership is still enforced by each statement, so a task whose
    # lease really was lost writes nothing either way.

    async def _succeed_one(self, ctx: TaskContext, result: Any) -> None:
        """Finalize one task the handler left for the worker to decide — the tail
        of both delivery modes.

        Includes the unserializable-result rule: the handler succeeded but its
        value cannot cross the JSON protocol, which is deterministic, so it fails
        permanently rather than being redelivered to fail the same way every
        attempt."""
        if ctx.settled:
            return
        try:
            # complete (not succeed): finalizes as canceled if a cancel was
            # requested while the handler ran, else succeeded.
            await self._store.complete(
                task_id=ctx.task_id, worker_id=self._worker_id, result=result
            )
            ctx._mark_settled()
        except LostLease:
            ctx._mark_lease_lost()
        except SerializationError as exc:
            await self._safe_fail(
                ctx,
                error_envelope(
                    type="SerializationError",
                    code="unserializable_result",
                    message=f"handler result is not JSON-serializable: {exc}",
                    retryable=False,
                ),
                retryable=False,
            )

    async def _settle_each(
        self,
        ctxs: list[TaskContext],
        settle: Callable[[TaskContext], Awaitable[None]],
    ) -> None:
        """Settle every task the handler did not settle itself, concurrently,
        reporting rather than raising. Each task keeps its own attempt count and
        backoff — they are separate tasks that happened to be delivered together.

        return_exceptions, because one task's write failing must not abandon the
        rest of the batch mid-settlement — the others still hold leases and would
        sit `running` until expiry. Each outcome is reported against the task it
        belongs to; without that, an operator learns a settlement failed somewhere
        in a batch of 256."""
        left = [c for c in ctxs if not c.settled]
        if not left:
            return
        outcomes = await asyncio.gather(
            *(settle(c) for c in left), return_exceptions=True
        )
        for ctx, outcome in zip(left, outcomes):
            if isinstance(outcome, BaseException):
                self._report(outcome, phase="execute", task_id=ctx.task_id)

    async def _attempt(
        self, invoke: Callable[[], Awaitable[Any]], ctxs: list[TaskContext]
    ) -> Any:
        """Run one attempt — of one task or of a whole batch — bounded by
        max_run_ms when set.

        On timeout the attempt is abandoned: every context it covers is flagged
        lease-lost first (so a handler that survives cancellation can never write
        again, nor settle anything behind the worker's back — see
        TaskContext._owned), then the handler task is cancelled and left to die at
        its next await. The caller records the handler_timeout failure; lease
        recovery is NOT involved, so redelivery is immediate."""
        if self._max_run_ms is None:
            return await invoke()
        runner = asyncio.create_task(invoke())
        done, _ = await asyncio.wait({runner}, timeout=self._max_run_ms / 1000)
        if done:
            return runner.result()  # or re-raises what the handler raised
        for ctx in ctxs:
            ctx._mark_lease_lost()
        runner.cancel()
        # Not awaited: a handler that shields itself from cancellation would
        # otherwise hold this slot for exactly the hang the ceiling exists for.
        runner.add_done_callback(_consume_result)
        raise _AttemptTimeout

    async def _heartbeat_loop(self, ctxs: list[TaskContext]) -> None:
        """One statement per beat, however many tasks the call covers — a
        single-task handler is just the one-element case.

        Only tasks still in play are renewed. A task the handler already settled
        is terminal, and re-leasing it would be a write against a row nobody owns;
        that is also why an absence is only read as lease loss after re-checking
        `settled`, since the handler may have finalized the task while this beat
        was in flight, which takes the row out of `running` and out of the reply.
        A task genuinely missing lost its lease (another worker recovered it), so
        its context is flagged and the handler stops being able to write through
        it — only that one, never its neighbours.
        """
        try:
            while True:
                await asyncio.sleep(self._hb_interval / 1000)
                live = [c for c in ctxs if not c.settled and not c.lost_lease]
                if not live:
                    return
                try:
                    renewed = await self._store.heartbeat_batch(
                        task_ids=[c.task_id for c in live],
                        worker_id=self._worker_id,
                        lease_ms=self._lease_ms,
                    )
                except Exception as exc:
                    self._report(exc, phase="execute", task_id=live[0].task_id)
                    continue
                for ctx in live:
                    if ctx.task_id in renewed:
                        # Cancellation rides along on the write we were making
                        # anyway, so ctx.canceled() stays free here too.
                        ctx._observe_cancel(renewed[ctx.task_id])
                    elif not ctx.settled:
                        ctx._mark_lease_lost()
        except asyncio.CancelledError:
            return

    async def _safe_fail(
        self, ctx: TaskContext, envelope: dict[str, Any], *, retryable: bool
    ) -> None:
        try:
            await self._store.fail(
                task_id=ctx.task_id,
                worker_id=self._worker_id,
                error=envelope,
                retryable=retryable,
                delay_ms=fail_delay_ms(
                    ctx.attempt,
                    retryable=retryable,
                    base_ms=self._retry_backoff_ms,
                    max_ms=self._retry_backoff_max_ms,
                ),
            )
            ctx._mark_settled()
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
