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

import asyncio
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from contextlib import AbstractAsyncContextManager
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Literal, get_args

if TYPE_CHECKING:  # import cycle: retention builds on the store it sweeps
    from ..retention import RetentionSweeper

from .._ids import new_id
from ..backpressure import (
    DEFAULT_MAX_WAIT_MS,
    INITIAL_PROBE_INTERVAL_MS,
    QueueDepthGate,
    QueueDepthLimit,
)
from ..errors import (
    AlreadyExists,
    LostLease,
    ProtocolVersionMismatch,
    SerializationError,
    UnsupportedBackend,
    error_envelope,
)
from ..models import STATUSES, TERMINAL, Task, TaskRef, TaskStatus

# Runs one named protocol statement and returns its rows.
Fetch = Callable[[str, dict[str, Any]], Awaitable[list[Any]]]

# Conflict is the canonical declaration; CONFLICTS derives from it (via
# get_args) so the runtime guard in submit() and the type can't drift apart
# (same pattern as TaskStatus/STATUSES in models.py).
Conflict = Literal["reuse", "reuse-succeeded", "reject", "replace"]
CONFLICTS: tuple[Conflict, ...] = get_args(Conflict)


def _reusable(conflict: Conflict, status: TaskStatus) -> bool:
    """Whether a keyed submit's strategy accepts the task the key already points
    at.

    Both reuse strategies deduplicate work that is still in play — that is what a
    key is for, and the answer cannot depend on the outcome of a task that has no
    outcome yet. They differ only on what a *finished* task means: `reuse` treats
    the key as free again, while `reuse-succeeded` reads a succeeded task as a
    cached result. Neither ever hands back a failed or canceled one, which would
    poison the key for every later submit (see PROTOCOL.md "Key conflict").
    """
    if conflict == "replace":
        return False
    if status not in TERMINAL:
        return True
    return conflict == "reuse-succeeded" and status == "succeeded"

SUPPORTED_PROTOCOL_MAJOR = 1


def check_protocol_version(version: int) -> None:
    """Refuse to run against a store whose protocol major this SDK does not
    speak. The supported major is a protocol fact, not a dialect one — every
    backend checks it here so the constant can't fork per store."""
    if version != SUPPORTED_PROTOCOL_MAJOR:
        raise ProtocolVersionMismatch(
            f"storage protocol_version={version}, SDK supports {SUPPORTED_PROTOCOL_MAJOR}"
        )


# allow_nan=False: the default encoder writes NaN/Infinity as bare ``NaN`` /
# ``Infinity`` — not JSON — which Python's lenient loads hides but JSON.parse in
# the TypeScript SDK throws on, poisoning the row for every cross-language
# reader. Compact separators match JSON.stringify, so the twins store the same
# bytes for the same value. A module-level encoder skips the fresh-encoder
# construction json.dumps pays for any non-default kwarg.
_ENCODER = json.JSONEncoder(allow_nan=False, separators=(",", ":"))


def dump_json(value: Any) -> str:
    """Encode a value for a protocol JSON column, raising SerializationError on
    anything JSON cannot represent (non-finite number, set, datetime, …)."""
    try:
        return _ENCODER.encode(value)
    except (TypeError, ValueError) as exc:
        raise SerializationError(str(exc)) from exc


def validate_purge_input(
    *, older_than_ms: int, status: TaskStatus | None, limit: int
) -> None:
    """Validate a purge's inputs. Shared with RetentionSweeper, which fail-fasts
    at construction on the same rules an hourly sweep would otherwise only
    surface through its on_error hook — one statement of the rules, two
    callers."""
    if older_than_ms < 0:
        raise ValueError(f"older_than_ms must be >= 0, got {older_than_ms}")
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")
    # Terminal only: purge never deletes live work, so accepting `queued` here
    # would be accepting a filter that silently matches nothing.
    if status is not None and status not in TERMINAL:
        raise ValueError(f"status must be terminal, got {status!r}")


LEASE_EXPIRED_ERROR_JSON = dump_json(
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


#: One draw inside a `claim_session`: (names, limit) -> the rows it took.
ClaimDraw = Callable[[list[str] | None, int], Awaitable[list[Task]]]


@dataclass(frozen=True, slots=True)
class WatchSignal:
    """Why `watch` is calling back.

    ``queued`` / ``done`` come from the store's push channel and name what
    moved; ``poll`` is the timer saying the watch cannot rule out a change. None
    of them carries state — the row is the truth.
    """

    reason: Literal["queued", "done", "poll"]
    #: The queue a task was queued on. Only on ``queued``.
    queue: str | None = None
    #: The task that reached a terminal status. Only on ``done``.
    task_id: str | None = None


#: How often `watch` signals in the absence of a push channel — and, where there
#: is one, how long a dropped listener can go unnoticed. The default trades a
#: dashboard's idle query rate against how stale it may look.
DEFAULT_WATCH_POLL_MS = 2_000


class TaskStore(ABC):
    #: Set by use_backpressure; None means submit is ungated.
    _gate: QueueDepthGate | None = None

    def use_backpressure(
        self,
        max_queue_depth: QueueDepthLimit,
        *,
        max_queue_wait_ms: int = DEFAULT_MAX_WAIT_MS,
        queue_poll_interval_ms: int = INITIAL_PROBE_INTERVAL_MS,
    ) -> None:
        """Bound how deep a queue may get before `submit` blocks. Off unless set.

        It hangs here rather than on `CairnQ` because the store is the one choke
        point every submit passes through — a handler spawning children via
        `TaskContext.submit` is the shape most likely to outrun its workers, and
        gating only the client would leave exactly that path unbounded."""
        self._gate = QueueDepthGate(
            self,
            max_queue_depth,
            max_queue_wait_ms=max_queue_wait_ms,
            queue_poll_interval_ms=queue_poll_interval_ms,
        )

    #: Set by use_retention; None means this store sweeps nothing.
    _sweeper: RetentionSweeper | None = None

    def use_retention(self, sweeper: RetentionSweeper) -> None:
        """Attach a retention sweep that begins when this store connects.

        It hangs here for the same reason backpressure does: the store is the one
        place every path reaches. Scheduling the sweep needs a running event loop,
        which a handle built at import time does not have, and `connect()` is
        optional — every operation connects lazily through it. Starting from the
        store is therefore the only place that cannot be skipped, and retention
        that silently depends on remembering a call is retention that silently
        does not happen. Only a CairnQ built with `retention` installs one."""
        self._sweeper = sweeper

    def _start_retention(self) -> None:
        """Called by each dialect at the end of connect(), where a loop is live."""
        if self._sweeper is not None:
            self._sweeper.start()

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

    def _subscribe_push(
        self, on_signal: Callable[[WatchSignal], None]
    ) -> Callable[[], None] | None:
        """Register for this store's push channel, if it has one; returns an
        unsubscribe. A store without a push channel returns None and `watch`
        degrades to its timer alone."""
        return None

    def _warm_push(self) -> None:
        """Nudge the push channel back up if it has dropped. Called from
        `watch`'s timer, which is the only thing keeping a client-side
        subscriber alive: a process that never claims never calls claim_wake, so
        without this a listener that died once would never come back there."""

    async def _has_claimable_work(self, params: dict[str, Any]) -> bool:
        """Whether it is worth opening the claim transaction at all. SQLite gates
        its single write lock behind a read-only probe; Postgres readers don't
        block writers, so it just says yes."""
        return True

    # ---------------------------------------------------------- wake channel
    # Wake-or-timeout contract (PROTOCOL.md "Push wakeups"): return when the
    # watched event may have happened, or after timeout_ms at the latest. The
    # default is a plain sleep — polling IS the wake mechanism; a dialect with
    # a push channel (PostgresStore, LISTEN/NOTIFY) returns earlier.

    async def claim_wake(self, queues: list[str], timeout_ms: int) -> None:
        """Returns when a task may have become claimable on one of `queues`."""
        await asyncio.sleep(timeout_ms / 1000)

    async def task_done_wake(self, task_id: str, timeout_ms: int) -> None:
        """Returns when `task_id` may have gone terminal (wait/call)."""
        await asyncio.sleep(timeout_ms / 1000)

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

    @staticmethod
    def _one_ref(rows: list[Any]) -> TaskRef | None:
        return TaskRef.from_row(rows[0]) if rows else None

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
        # max_attempts < 1 would still run once (claim increments before the
        # check), a silently different meaning than the number says; a negative
        # delay is always a mistake. Both fail loudly instead.
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        if run_at_delay_ms < 0:
            raise ValueError(f"run_at_delay_ms must be >= 0, got {run_at_delay_ms}")
        # After validation and before the first write: bad arguments should fail
        # now, not after waiting out a full queue.
        if self._gate is not None:
            await self._gate.acquire(queue)
        task_id = new_id("task")
        ins = {
            "id": task_id,
            "name": name,
            "queue": queue,
            "payload": dump_json(payload if payload is not None else {}),
            "metadata": dump_json(metadata or {}),
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
                    if conflict == "reject":
                        raise AlreadyExists(key)
                    if _reusable(conflict, rows[0]["status"]):
                        return Task.from_row(rows[0])
                    # The strategy declined the recorded task, so the key repoints
                    # to the fresh one inserted below. Cancel only what is still
                    # live: a terminal task has nothing to stop, and cancelling it
                    # would rewrite a settled row (and hand a `canceled` back to
                    # whoever is waiting on it).
                    if rows[0]["status"] not in TERMINAL:
                        await fetch("cancel", {"id": existing[0]["task_id"]})
            rows = await fetch("insert_task", ins)
            await fetch("upsert_key", {"key": key, "task_id": task_id})
            return Task.from_row(rows[0])

    async def get(self, task_id: str) -> Task | None:
        return self._one(await self._fetch("get", {"id": task_id}))

    async def get_by_key(self, key: str) -> Task | None:
        return self._one(await self._fetch("get_by_key", {"key": key}))

    async def get_status(self, task_id: str) -> TaskRef | None:
        """The wait loop's probe: id + status alone, so polling a task with a
        large payload does not re-read and re-parse that payload on every beat."""
        return self._one_ref(await self._fetch("get_status", {"id": task_id}))

    async def get_status_by_key(self, key: str) -> TaskRef | None:
        """get_status, following a key instead of an id — re-resolved per call,
        so a `replace` moves the probe onto the new task."""
        return self._one_ref(await self._fetch("get_status_by_key", {"key": key}))

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
        # Validate up front, like submit's conflict guard: a typo'd status
        # otherwise matches nothing and returns [] indistinguishably from
        # "no such tasks".
        if status is not None and status not in STATUSES:
            raise ValueError(f"unknown status filter: {status!r}")
        if limit < 0 or offset < 0:
            raise ValueError(f"limit/offset must be >= 0, got limit={limit} offset={offset}")
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

    async def purge(
        self,
        *,
        older_than_ms: int = 0,
        status: TaskStatus | None = None,
        name: str | None = None,
        limit: int = 1_000,
    ) -> list[str]:
        """Delete terminal tasks that completed more than `older_than_ms` ago and
        return their ids. Nothing else removes rows, so a long-lived database
        needs this called periodically. Bounded by `limit` to keep each sweep a
        short write; call it in a loop until it returns fewer than `limit`.

        `status` / `name` restrict the sweep to one terminal status or task name.
        Retention needs are tiered — a succeeded row is spent once its result is
        consumed, while a failed one is worth keeping for diagnosis — and without
        them the shortest-lived tier sets the retention for every row."""
        validate_purge_input(older_than_ms=older_than_ms, status=status, limit=limit)
        rows = await self._fetch(
            "purge",
            {"older_than_ms": older_than_ms, "status": status, "name": name, "limit": limit},
        )
        return [r["id"] for r in rows]

    async def stats(self) -> dict[str, dict[TaskStatus, int]]:
        """Task counts per queue, keyed by status and zero-filled across all
        statuses — `stats()["default"]["queued"]` is the backlog of a queue.
        A queue appears only while it has rows; terminal tasks keep counting
        until `purge` removes them."""
        out: dict[str, dict[TaskStatus, int]] = {}
        for row in await self._fetch("stats", {}):
            per = out.setdefault(row["queue"], dict.fromkeys(STATUSES, 0))
            per[row["status"]] = int(row["count"])
        return out

    def watch(
        self,
        on_signal: Callable[[WatchSignal], None],
        *,
        queues: list[str] | tuple[str, ...] | None = None,
        poll_ms: int = DEFAULT_WATCH_POLL_MS,
    ) -> Callable[[], None]:
        """Call `on_signal` when the tasks on `queues` may have changed —
        something was queued, or something finished.

        This is notify-ACCELERATED POLLING, not an event log, and the difference
        is the whole contract. Where a push channel is available (Postgres
        LISTEN) an idle watch costs nothing and a signal arrives within
        milliseconds of the event. Where it is not — a transaction-mode pooler
        refuses LISTEN, SQLite has no channel at all — the timer alone still
        delivers ``poll`` signals, so a consumer that re-reads on every signal is
        correct in both cases and merely less prompt in one.

        What it will NOT do is promise that a signal means something happened, or
        that every event produces its own signal. Treat a signal as "re-read now"
        and take the truth from `stats` / `list` / `get`, which is where it
        lives. ``reason`` is a hint for reading less: a ``done`` signal names the
        task, so a dashboard can refresh that row instead of the list.

        Returns an unsubscribe. Mirrors `watch` in the TypeScript SDK.
        """
        interval = max(1, poll_ms) / 1000
        wanted = set(queues) if queues else None
        live = True

        def emit(signal: WatchSignal) -> None:
            # A signal delivered after unsubscribe would have the consumer
            # re-reading a store it has stopped caring about, possibly a closed
            # one.
            if live:
                on_signal(signal)

        def filtered(signal: WatchSignal) -> None:
            # A queued signal names its queue, so a watch scoped to some queues
            # can drop the rest. A done signal names only the task — which queue
            # it was on is not in the notification, so it is never filtered out.
            if signal.reason == "queued" and wanted is not None and signal.queue not in wanted:
                return
            emit(signal)

        unsubscribe = self._subscribe_push(filtered)

        async def tick() -> None:
            while True:
                await asyncio.sleep(interval)
                self._warm_push()
                # Guarded for the same reason PostgresStore guards its push
                # fan-out: a consumer that raises must not take the timer down
                # with it. Losing the timer would silently retire the fallback
                # that makes watch correct where there is no push channel.
                try:
                    emit(WatchSignal(reason="poll"))
                except Exception:
                    pass  # the consumer's problem, not the watch's

        timer = asyncio.get_running_loop().create_task(tick())

        def stop() -> None:
            nonlocal live
            live = False
            if unsubscribe is not None:
                unsubscribe()
            timer.cancel()

        return stop

    async def queue_depth(self, queue: str, max_depth: int) -> int:
        """How many more tasks fit on `queue` under `max_depth` — 0 once it is
        full.

        The cheap half of backpressure: bounded at `max_depth` index entries,
        unlike `stats()`, which aggregates the whole table (terminal rows
        included) and so costs more the longer a database has been running. Use
        it directly to shed load or shape a producer; `QueueDepthGate` builds the
        blocking form on top."""
        if max_depth < 0:
            raise ValueError(f"max_depth must be >= 0, got {max_depth}")
        rows = await self._fetch("queue_depth", {"queue": queue, "max_depth": max_depth})
        return int(rows[0]["headroom"]) if rows else 0

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
        name_list = None if names is None else list(names)
        claimed = await self.claim_session(
            queues=queues,
            worker_id=worker_id,
            lease_ms=lease_ms,
            names=name_list,
            plan=lambda claim: claim(name_list, limit),
        )
        return claimed if claimed is not None else []

    async def claim_session(
        self,
        *,
        queues: list[str],
        worker_id: str,
        lease_ms: int = 30_000,
        names: list[str] | None,
        plan: Callable[[ClaimDraw], Awaitable[Any]],
    ) -> Any:
        """Open one claim transaction and let the caller draw from it repeatedly.

        The transaction is what has to live here: the read-only probe that keeps
        an idle worker off SQLite's single write lock, the `recover_leases` whose
        reclaimed leases must be visible to the claims that follow and to nobody
        in between, and the write lock itself. *What* gets claimed under it is the
        caller's business — a worker drawing a separate quota per task name is
        scheduling policy, and this layer has no vocabulary for the "handler call"
        that policy is denominated in. It knows queues, names, limits and rows.

        `plan` is handed a `claim(names, limit)` it may await any number of times,
        each a separate statement under the same lock and the same recovery, and
        each free to size itself from what the previous one returned. That
        feedback is the reason this is a callback rather than a list of quotas: a
        caller dividing a budget up front has to guess, and every share handed to
        a name with nothing queued is a slot left idle until the next poll.

        `plan` runs with the write lock held, so it must await nothing but that
        callback.

        `names` is the union `plan` might ask for — the probe and the recovery are
        filtered by it. Returns None when the probe finds nothing claimable, in
        which case `plan` never runs and no transaction is opened."""
        # A list-valued filter cannot be read in claim order, so the planner sorts
        # every claimable row to take LIMIT of them and the claim's cost grows
        # with the backlog while it holds the transaction. Both filters therefore
        # have an equality form, picked per draw: one queue is the common
        # deployment, and one name is every per-name quota. See
        # claim_one_queue.sql and claim_one_name.sql.
        queue_list = list(queues)
        one_queue = len(queue_list) == 1
        base = {
            "queues": queue_list,
            "queue": queue_list[0] if one_queue else None,
            "names": names,
            "name": None,
            "worker_id": worker_id,
            "lease_ms": lease_ms,
            "limit": 1,
            "lease_expired_error": LEASE_EXPIRED_ERROR_JSON,
        }
        if not await self._has_claimable_work(base):
            return None
        # Recovery must share the claim's transaction: a lease reclaimed here has
        # to be visible to the claims that follow, and to nobody in between.
        async with self._transaction() as fetch:
            await fetch("recover_leases", base)

            async def claim(draw_names: list[str] | None, limit: int) -> list[Task]:
                # A draw asking for nothing, or filtered to no names, claims
                # nothing — answer it here rather than spending a statement to
                # learn that.
                if limit <= 0 or draw_names == []:
                    return []
                one_name = draw_names is not None and len(draw_names) == 1
                if one_name:
                    statement = "claim_one_queue_one_name" if one_queue else "claim_one_name"
                else:
                    statement = "claim_one_queue" if one_queue else "claim"
                rows = await fetch(
                    statement,
                    {
                        **base,
                        "names": draw_names,
                        "name": draw_names[0] if one_name else None,
                        "limit": limit,
                    },
                )
                return [Task.from_row(r) for r in rows]

            return await plan(claim)

    async def heartbeat(self, *, task_id: str, worker_id: str, lease_ms: int = 30_000) -> Task:
        return await self._owned_write(
            "heartbeat", task_id, {"id": task_id, "worker_id": worker_id, "lease_ms": lease_ms}
        )

    async def heartbeat_batch(
        self, *, task_ids: list[str], worker_id: str, lease_ms: int = 30_000
    ) -> dict[str, bool]:
        """Renew several leases in one statement. Returns `{task_id: cancel
        requested}` for the tasks this worker still holds.

        Deliberately not an `_owned_write`: ownership is per task here, so there
        is no single answer to "did it work". A task **absent** from the result
        lost its lease, and the caller decides what that means for that one task
        rather than failing the whole beat.

        It returns flags rather than `Task`s because nothing downstream needs a
        task: the caller renews leases and observes cancellation, and whole rows
        would drag every payload back on every beat for the life of the call."""
        if not task_ids:
            return {}
        rows = await self._fetch(
            "heartbeat_batch",
            {"ids": list(task_ids), "worker_id": worker_id, "lease_ms": lease_ms},
        )
        return {r["id"]: r["cancel_requested_at_ms"] is not None for r in rows}

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
                "result": None if result is None else dump_json(result),
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
                "result": None if result is None else dump_json(result),
            },
        )

    def _transaction_with_session(
        self,
    ) -> AbstractAsyncContextManager[tuple[Fetch, Any]]:
        """`_transaction`, but also handing out the driver's own session so the
        CALLER can run their statements in the same transaction as the
        protocol's.

        This is what lets a task's settlement and the rows that task produced
        commit together. Without it the two are separate transactions and there
        is a window where the work is durable but the task still reads as
        unfinished — a crash there costs a full recomputation on retry, and for
        non-idempotent work costs more than that.

        Optional, because the session type is the driver's, not the protocol's: a
        store with no session worth handing out does not override it, and
        `complete_in` detects that by identity rather than by calling it.
        Postgres implements it; SQLite does not.
        """
        raise UnsupportedBackend(f"{type(self).__name__} has no driver session to share")

    async def complete_in(
        self,
        *,
        task_id: str,
        worker_id: str,
        write: Callable[[Any], Awaitable[Any]],
    ) -> tuple[Task, Any]:
        """`complete`, with the caller's own writes committed in the same
        transaction.

        `write` runs first and whatever it returns becomes the task's result; the
        settlement is the last statement in the transaction. So a lost lease —
        the settlement matching no row — rolls the caller's writes back with it,
        and there is no ordering in which the work is recorded but the task is
        not.

        The settlement runs LAST rather than checking ownership up front on
        purpose: the ownership predicate lives in the protocol's complete.sql,
        and a fail-fast pre-check here would be a second copy of it, free to
        drift. The cost of that choice is that a doomed attempt does its work
        before finding out, which is the rare path.
        """
        # Existence, not behavior: mirrors the TypeScript SDK's `if
        # (!this.txWithSession)`. Catching NotImplementedError out of the call
        # would also swallow one raised from INSIDE a real implementation and
        # report it as "this backend cannot", which is a different and much more
        # confusing failure.
        if type(self)._transaction_with_session is TaskStore._transaction_with_session:
            raise UnsupportedBackend(
                "this store cannot share a transaction with the caller — "
                "complete_in requires a Postgres store (see PgExecutor)"
            )
        async with self._transaction_with_session() as (fetch, session):
            value = await write(session)
            rows = await fetch(
                "complete",
                {
                    "id": task_id,
                    "worker_id": worker_id,
                    "result": None if value is None else dump_json(value),
                },
            )
            # Rolls back `write`'s writes along with the settlement that did not
            # land.
            if not rows:
                raise LostLease(task_id)
            return Task.from_row(rows[0]), value

    async def fail(
        self,
        *,
        task_id: str,
        worker_id: str,
        error: dict[str, Any],
        retryable: bool = True,
        delay_ms: int = 0,
    ) -> Task:
        try:
            error_text = dump_json(error)
        except SerializationError:
            # A failure record must never itself fail to serialize (a TaskError
            # carrying exotic details would otherwise strand the task until
            # lease expiry). Strip the envelope to its string fields and record
            # that.
            error_text = dump_json(
                error_envelope(
                    type=str(error.get("type", "TaskError")),
                    code=str(error.get("code", "task_error")),
                    message=str(error.get("message", "")),
                    retryable=retryable,
                )
            )
        return await self._owned_write(
            "fail",
            task_id,
            {
                "id": task_id,
                "worker_id": worker_id,
                "error": error_text,
                "retryable": retryable,
                "delay_ms": delay_ms,
            },
        )
