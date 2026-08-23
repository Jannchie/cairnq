from __future__ import annotations

import asyncio
import contextlib
import inspect
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
from .backpressure import DEFAULT_MAX_WAIT_MS, QueueDepthLimit
from .context import TaskContext
from .errors import (
    LostLease,
    SerializationError,
    as_envelope,
    error_envelope,
)
from .models import Task, TaskDef, task_name
from .store.base import TaskStore
from .store.pg_executor import PgExecutor
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


def _is_async_handler(fn: Handler) -> bool:
    """Whether calling `fn` hands back a coroutine to await, rather than doing the
    work on the caller's thread. Decided once at registration, since the answer is
    static and the hot path must not re-derive it."""
    if inspect.iscoroutinefunction(fn):
        return True
    # A callable object is a handler too, and its asyncness lives on __call__.
    call = getattr(type(fn), "__call__", None)
    return call is not None and inspect.iscoroutinefunction(call)


async def _call(handler: Handler, is_async: bool, *args: Any) -> Any:
    """Call a handler — the one place that decides sync handlers are as welcome
    as async ones.

    **A sync handler runs on a worker thread, never on the event loop.** The loop
    is what renews this task's lease, and a handler that occupies it for seconds
    at a time — a GPU forward, a hash over a large file, a synchronous HTTP
    client — starves the heartbeat until the lease expires and another worker
    picks the task up while this one is still computing it. That failure is
    invisible from inside the handler, so it is not left to the caller to avoid.
    The threads come from asyncio's default executor, which bounds how many sync
    handlers run at once independently of `concurrency`.

    Two consequences worth knowing. A sync handler cannot await `ctx` (its methods
    are coroutines), which was already true when it ran on the loop. And a thread
    cannot be cancelled, so `max_run_ms` stops *waiting* for a sync handler
    without stopping the handler — the attempt is abandoned and the thread runs to
    completion.

    The argument list is the calling convention, and there are three: a
    single-task handler receives `(ctx, payload)` (the whole payload dict —
    destructure it yourself), or `(ctx)` alone when it declares no payload
    parameter, and a batch handler receives one argument, the list of contexts.
    There is no payload shortcut to pair with the batch form: payloads are per
    task, so they are read off the items (`item.payload`), which is also what a
    handler must hold to settle one of them."""
    if is_async:
        return await handler(*args)
    result = await asyncio.to_thread(handler, *args)
    # A sync function may still hand back an awaitable (a partial around a
    # coroutine function, a callable returning one); await it here, on the loop.
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class _Registration:
    """What `worker.task` recorded for one task name."""

    fn: Handler
    #: Whether a single-task handler declared a payload parameter, decided once
    #: from the signature so the hot path never re-inspects it.
    wants_payload: bool
    #: Whether the handler is a coroutine function. A sync one is dispatched to a
    #: thread instead of running on the loop — see `_call`.
    is_async: bool
    #: Tasks per handler call, or None for one-at-a-time delivery.
    batch: int | None = None
    #: Resource this name's calls draw from, or None to draw from nothing but
    #: the worker budget. Declared in `Worker(resources=...)`.
    resource: str | None = None


@dataclass(frozen=True)
class _ClaimSource:
    """One draw's worth of quota: a set of names and how many handler calls they
    may start.

    A name that limits itself — by `batch`, or by a `resource` it shares with
    other names — gets a source to itself, because its quota cannot be expressed
    in a draw shared with names that count differently. Everything else shares
    one, where a task is a call."""

    names: tuple[str, ...]
    #: Tasks per call — 1 for the shared source.
    batch: int
    #: Resource this source draws from, or None. The ceiling it names is shared
    #: with the other sources that declare it, which is what keeps two names off
    #: one scarce thing at the same time.
    resource: str | None = None


class _ClaimPlanner:
    """The worker's claim-scheduling state, in one object: which names draw
    together and at what batch size, which shared resource each draw spends, how
    many calls each resource still has room for, and whose turn it is.

    A name that limits itself — by `batch`, or by a `resource` — needs a quota
    the shared draw cannot express, so it gets a source of its own; every other
    name shares one, where a task is a call.

    A resource is deliberately *not* one source spanning its names: `batch` is
    per name, and a single source carries one batch size, so two members that
    batch differently could not share a draw. Keeping a source per name and
    letting several of them draw down one shared ceiling composes with batching
    instead of excluding it.
    """

    def __init__(self, capacities: dict[str, int]):
        self._capacities = capacities
        self._sources: tuple[_ClaimSource, ...] = ()
        #: The union of names the claim probe spans.
        self.names: list[str] = []
        #: Calls holding units of each declared resource.
        self._in_flight: dict[str, int] = {}
        #: Rotates which source is offered the free budget first — see plan().
        self._cursor = 0

    def rebuild(self, handlers: dict[str, _Registration]) -> None:
        """Rebuild the sources from the registrations. Called from register(),
        which is the only place the set changes, so a poll never pays for it."""
        sources: list[_ClaimSource] = []
        shared: list[str] = []
        for name, reg in handlers.items():
            if reg.batch is not None or reg.resource is not None:
                sources.append(_ClaimSource((name,), reg.batch or 1, reg.resource))
            else:
                shared.append(name)
        if shared:
            sources.append(_ClaimSource(tuple(shared), 1))
        self._sources = tuple(sources)
        self.names = list(handlers)

    @property
    def empty(self) -> bool:
        return not self._sources

    async def plan(
        self,
        free: int,
        claim: Callable[[list[str] | None, int], Awaitable[list[Task]]],
        split: Callable[[list[Task]], list[tuple["_Registration | None", list[Task]]]],
    ) -> list[tuple[_ClaimSource, list[tuple["_Registration | None", list[Task]]]]]:
        """Draw up to `free` handler calls from one claim transaction.

        Sources are served in rotating order — without rotating, the first source
        would take every free slot and the rest would starve behind its backlog.
        Each draw sizes itself from what the previous ones took, so a source with
        nothing queued costs nothing; that feedback is why this runs inside the
        claim transaction rather than dividing the budget up front. It must await
        nothing but `claim` — the store holds the write lock while it runs.

        `split` is the worker's `_deliveries`: rows become handler calls there, so
        spending the budget against its result is the only way the two cannot
        disagree."""
        cursor = self._cursor % len(self._sources)
        order = self._sources[cursor:] + self._sources[:cursor]
        self._cursor = (cursor + 1) % len(self._sources)
        drawn: list[tuple[_ClaimSource, list[tuple[_Registration | None, list[Task]]]]] = []
        left = free
        # Resource units this poll has already drawn. `_in_flight` only moves
        # when a call is dispatched, which happens after this whole plan returns
        # — so without a local tally two sources sharing a resource would each
        # see its full ceiling and together overshoot it.
        taken: dict[str, int] = {}
        for src in order:
            if left <= 0:
                break
            own = self._headroom(src, taken)
            quota = left if own is None else min(own, left)
            if quota <= 0:
                continue
            rows = await claim(list(src.names), src.batch * quota)
            if not rows:
                continue
            calls = split(rows)
            drawn.append((src, calls))
            left -= len(calls)
            if src.resource is not None:
                taken[src.resource] = taken.get(src.resource, 0) + len(calls)
        return drawn

    def _headroom(self, src: _ClaimSource, taken: dict[str, int]) -> int | None:
        """A source's own call ceiling for one poll, or None when only the
        worker-wide budget applies: its resource's capacity less what is running
        and what earlier draws in this same poll already took."""
        if src.resource is None:
            return None
        return max(
            0,
            self._capacities[src.resource]
            - self._in_flight.get(src.resource, 0)
            - taken.get(src.resource, 0),
        )

    def acquire(self, resource: str | None) -> None:
        """Charge one dispatched call to its resource, for the span of the call."""
        if resource is None:
            return
        self._in_flight[resource] = self._in_flight.get(resource, 0) + 1

    def release(self, resource: str | None) -> None:
        """Give the unit back. Popping at zero keeps the dict to the resources
        actually in flight, so an idle worker holds no entries at all."""
        if resource is None:
            return
        rest = self._in_flight.get(resource, 1) - 1
        if rest > 0:
            self._in_flight[resource] = rest
        else:
            self._in_flight.pop(resource, None)


class Worker:
    def __init__(
        self,
        store: TaskStore,
        queues: list[str] | tuple[str, ...],
        *,
        concurrency: int = 1,
        lease_ms: int = 30_000,
        poll_interval_ms: int = 500,
        retry_backoff_ms: int = DEFAULT_RETRY_BACKOFF_MS,
        retry_backoff_max_ms: int = DEFAULT_RETRY_BACKOFF_MAX_MS,
        max_run_ms: int | None = None,
        resources: dict[str, int] | None = None,
        # Backpressure is accepted here too, not only on CairnQ: a handler
        # spawning children through TaskContext.submit is a producer, and in a
        # worker process there is usually no CairnQ handle to have configured
        # the store.
        max_queue_depth: QueueDepthLimit | None = None,
        max_queue_wait_ms: int = DEFAULT_MAX_WAIT_MS,
        on_error: OnError | None = None,
    ):
        if max_run_ms is not None and max_run_ms <= 0:
            raise ValueError(f"max_run_ms must be > 0, got {max_run_ms}")
        for resource, capacity in (resources or {}).items():
            if capacity < 1:
                raise ValueError(
                    f"resources[{resource!r}] must be >= 1, got {capacity}"
                )
        if max_queue_depth is not None:
            store.use_backpressure(max_queue_depth, max_queue_wait_ms=max_queue_wait_ms)
        self._store = store
        self._queues = list(queues)
        self._concurrency = concurrency
        self._lease_ms = lease_ms
        # lease/3 gives two beats of slack; the floor only matters for sub-150ms leases.
        self._hb_interval = max(50, lease_ms // 3)
        self._poll = poll_interval_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._retry_backoff_max_ms = retry_backoff_max_ms
        # Wall-clock ceiling for one attempt. The heartbeat renews the lease for
        # as long as a handler runs, so a hung handler would otherwise hold its
        # task `running` (and its concurrency slot) forever — cancel can't help,
        # cooperative checks need a live handler. None disables the ceiling.
        self._max_run_ms = max_run_ms
        # Which names draw together, what each draw spends, and whose turn it is.
        # Capacities are the declared resources: ceilings several names may draw
        # from, which is what expresses "these handlers share one GPU" without
        # inventing a queue per resource.
        self._resources = dict(resources or {})
        self._planner = _ClaimPlanner(self._resources)
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
        source: str | PgExecutor,
        *,
        queues: list[str] | tuple[str, ...] = ("default",),
        concurrency: int = 1,
        lease_ms: int = 30_000,
        min_size: int = 1,
        max_size: int = 10,
        schema: str | None = None,
        **kwargs: Any,
    ) -> "Worker":
        """Multi-host backend. `source` is a libpq connection string — which
        requires the optional asyncpg package (install cairnq[postgres]) — or a
        PgExecutor over a driver the application already runs.

        `schema` is the schema cairnq's tables live in — arranged by cairnq on
        the DSN path, only asserted on the executor path — and must match every
        other process in the deployment. See CairnQ.postgres."""
        worker = cls(
            PostgresStore(source, min_size=min_size, max_size=max_size, schema=schema),
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
        self,
        name: str | TaskDef[Any, Any] | Handler | None = None,
        *,
        batch: int | None = None,
        resource: str | None = None,
    ) -> Any:
        """Register a handler. Usable bare (`@worker.task` — registered under the
        function's name), with an explicit name (`@worker.task("summary.create")`)
        for dotted/namespaced or cross-language matching, or with a TaskDef
        (`@worker.task(my_task)`) so the name is shared with the submit side.

        `batch=N` switches this name to batch delivery: the handler takes one
        argument, a `list[TaskContext]` of up to N tasks, instead of
        `(ctx, payload)`. Use it when the work itself is batched — one embedding
        call over 256 texts rather than 256 calls — and size N by what the
        downstream API wants, not by the queue.

        `resource="gpu"` draws each call from a ceiling declared in
        `Worker(resources={"gpu": 1})` and shared with every other name that
        names it. Use it when several handlers contend for one scarce thing —
        a GPU, an index that tolerates a single writer — and the limit belongs
        to the thing rather than to any one of them. At capacity 1 it is
        mutual exclusion across those names. A name that only needs to cap
        itself declares a resource of its own."""
        if callable(name):  # used bare: @worker.task
            self.register(_required_name(name), name, batch=batch, resource=resource)
            return name

        resolved = None if name is None else task_name(name)

        def decorator(fn: Handler) -> Handler:
            self.register(resolved or _required_name(fn), fn, batch=batch, resource=resource)
            return fn

        return decorator

    def register(
        self,
        name: str,
        fn: Handler,
        *,
        batch: int | None = None,
        resource: str | None = None,
    ) -> None:
        if batch is not None and batch < 1:
            raise ValueError(f"batch must be >= 1, got {batch}")
        # Loudly, at registration: an undeclared resource would otherwise read
        # as an unbounded one, so a typo would silently remove the ceiling the
        # caller asked for — the failure this option exists to prevent.
        if resource is not None and resource not in self._resources:
            known = ", ".join(sorted(self._resources)) or "none"
            raise ValueError(
                f"task {name!r} declares resource {resource!r}, which is not in "
                f"Worker(resources=...); declared: {known}"
            )
        self._handlers[name] = _Registration(
            fn, _wants_payload(fn), _is_async_handler(fn), batch, resource
        )
        self._planner.rebuild(self._handlers)

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
        # The calls in flight — `concurrency` counts these, so the set's size is
        # the budget. How many tasks they carry is the batch sizes' business.
        running: set[asyncio.Task] = set()
        try:
            while not self._stop.is_set():
                # `concurrency` counts calls, so the calls in flight *are* the
                # running tasks — a batch holding 256 tasks is one of them.
                free = concurrency - len(running)
                if free <= 0:
                    # Wait for a slot rather than polling for one. _run_call never
                    # raises, so waiting on it cannot surface an exception here.
                    await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                    continue
                if self._planner.empty:
                    await self._idle(self._poll)
                    continue
                try:
                    claimed = await self._store.claim_session(
                        queues=self._queues,
                        worker_id=self._worker_id,
                        lease_ms=self._lease_ms,
                        # Only what this worker can run. Queues do not partition
                        # work by task name, so another worker's tasks would
                        # otherwise be claimed here and failed for want of a
                        # handler.
                        names=self._planner.names,
                        plan=lambda claim, free=free: self._planner.plan(
                            free, claim, self._deliveries
                        ),
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
                for src, calls in claimed:
                    for reg, group in calls:
                        # Charged before the handler starts and refunded when it
                        # settles, so the resource covers exactly the span the
                        # call holds its slot.
                        self._planner.acquire(src.resource)

                        def settled(
                            fut: asyncio.Task, resource: str | None = src.resource
                        ) -> None:
                            running.discard(fut)
                            self._planner.release(resource)

                        # An unregistered name has nothing to run, so it never
                        # starts a handler or a heartbeat; everything else is one
                        # call, batched or not.
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
        try:
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
        except Exception as exc:  # noqa: BLE001 - reported, never fatal
            # Reported rather than swallowed, as in the TS twin: this is a store
            # write that failed while finalizing a task, which on_error exists to
            # surface. Swallowing it leaves the task to sit until its lease
            # expires with nothing anywhere saying why.
            self._report(exc, phase="execute", task_id=task.id)

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
                result = await self._attempt(lambda: _call(reg.fn, reg.is_async, *args), ctxs)
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
        """Run the worker in the same process (deployment mode A).

        ``run()`` can fail before the body has done anything — a database that
        will not open, a migration that will not apply, a protocol version this
        SDK does not speak. Suppressing that (as this used to) left the block
        looking like it succeeded while no worker was running at all, and nothing
        anywhere said why. Only cancellation is swallowed now.

        If the body returned, the worker's failure is what the caller hears: a
        result produced with no worker running is not a success worth reporting
        quietly. If the body raised, its own error wins — it is the caller's
        code, and replacing it would hide what they were actually doing — with
        the worker's failure attached as ``__cause__`` so the root of it stays
        reachable. Mirrors background() in the TypeScript SDK.
        """
        runner = asyncio.create_task(self.run(concurrency=concurrency))

        async def shutdown() -> BaseException | None:
            """Stop the worker and hand back what run() failed with, if anything."""
            self.stop()
            failure: BaseException | None = None
            try:
                await runner
            except asyncio.CancelledError:
                pass
            except BaseException as exc:  # noqa: BLE001 - re-raised by the caller below
                failure = exc
            await self._close_if_owned()
            return failure

        try:
            yield self
        except BaseException as exc:
            failure = await shutdown()
            if failure is not None and exc.__cause__ is None:
                exc.__cause__ = failure
            raise
        failure = await shutdown()
        if failure is not None:
            raise failure
