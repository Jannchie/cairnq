"""PostgresStore — the Postgres dialect of the shared cairnq-protocol SQL.

Everything protocol-shaped lives in TaskStore; this file is only what Postgres
does differently: an asyncpg pool, `:name` -> `$n` translation, and time taken
from the DB clock (`now()`) instead of from the SDK, which is what makes this
backend multi-host — unlike SQLite it coordinates API and worker processes across
machines, with no shared clock to agree on. claim uses FOR UPDATE SKIP LOCKED and
needs no claimable_probe, because PG readers don't block writers.

asyncpg is an optional dependency (install ``cairnq[postgres]``); it is imported
lazily in __init__ so the rest of the SDK works without it."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from functools import lru_cache
from typing import Any

from ..errors import SchemaMismatch
from .._sql import load_migrations, load_statements
from .pg_executor import ListenUnavailable, PgExecutor, PgSession
from .pg_pool import check_schema_name, create_pool_executor
from .base import (
    COMMENT,
    NAMED,
    Fetch,
    TaskStore,
    WatchSignal,
    check_protocol_version,
    statement_params,
)

# Notification channels, emitted by the 0003_notify trigger.
QUEUED_CHANNEL = "cairnq_queued"
DONE_CHANNEL = "cairnq_done"

# Backoff between attempts to (re)connect the LISTEN connection after a
# transient failure. Doubles per failure up to the cap; polling covers the gap.
LISTENER_RETRY_MS = 1_000
LISTENER_RETRY_MAX_MS = 30_000


@lru_cache(maxsize=None)
def positional_statement(sql: str) -> tuple[str, tuple[str, ...]]:
    """The rewritten SQL and the order its `$n` slots must be filled in.

    Translates the protocol's named-parameter SQL (`:name`) into asyncpg
    positional placeholders (`$1`), collapsing each DISTINCT name to ONE slot —
    statements reuse a name across CASE branches / IS NULL guards (e.g. list.sql).
    Which names count as parameters is `statement_params`' decision, shared with
    the SQLite binding path so the two can't disagree about, say, a `::type` cast.

    Memoized on the statement text, which is loaded once and never varies: this
    runs on every query, including the worker's poll loop, for a result that
    cannot have changed.
    """
    order = statement_params(sql)
    slot = {name: i + 1 for i, name in enumerate(order)}  # 1-based $n
    return NAMED.sub(lambda m: f"${slot[m.group(1)]}", COMMENT.sub("", sql)), order


def to_positional(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """The statement's rewritten text plus this call's values, in slot order.
    Names the statement does not use are simply not bound, so callers may pass a
    superset."""
    text, order = positional_statement(sql)
    return text, [params[n] for n in order]


class PostgresStore(TaskStore):
    def __init__(
        self,
        source: str | PgExecutor,
        *,
        min_size: int = 1,
        max_size: int = 10,
        schema: str | None = None,
    ):
        """`source` is either a libpq connection string — this store then owns an
        asyncpg pool and requires the optional asyncpg package — or a PgExecutor
        the caller already has, which this store uses and never closes."""
        if schema is not None:
            check_schema_name(schema)
        self._dsn = source if isinstance(source, str) else None
        # The caller's executor, if one was injected — never closed by this store.
        self._provided: PgExecutor | None = None if isinstance(source, str) else source
        # Set once migrations have run and the protocol version checked out.
        self._executor: PgExecutor | None = None
        self._min_size = min_size
        self._max_size = max_size
        self._schema = schema
        self._init_lock = asyncio.Lock()
        self._sql = load_statements("postgres")
        # LISTEN/NOTIFY state. One dedicated connection listens on both channels
        # (see 0003_notify.sql and the claim_wake/task_done_wake contract on
        # TaskStore). Failure to establish or keep it silently degrades the
        # store to the base class's plain polling.
        self._listener: Any = None
        self._listener_connecting: asyncio.Task | None = None
        # LISTEN is off for good: the store was closed, or the server accepted
        # a connection but refused LISTEN (e.g. a transaction-mode pooler) —
        # deterministic, so retrying would fail the same way every time.
        self._listener_unavailable = False
        # A failure to even connect is transient (network blip, server
        # restarting): retry, but not before this loop-clock time, backing off
        # so a down server is not hammered from the poll loop.
        self._listener_retry_at = 0.0
        self._listener_backoff_ms = LISTENER_RETRY_MS
        # Queues notified while nobody was waiting; consumed by the next
        # claim_wake so a wake that lands between polls is not lost. The event
        # broadcasts "some queue was notified"; waiters filter for themselves.
        self._pending_queues: set[str] = set()
        self._queued_event = asyncio.Event()
        self._done_waiters: dict[str, set[asyncio.Event]] = {}
        # watch() subscribers. Separate from the wake events: a waiter is
        # one-shot and consumes the notification, a subscriber is standing and
        # only observes.
        self._subscribers: set[Callable[[WatchSignal], None]] = set()

    # ------------------------------------------------------------------ setup
    async def connect(self) -> None:
        if self._executor is not None:
            return
        # The lock makes concurrent first-touch operations share one executor
        # instead of each racing to create its own (double-check after acquiring).
        async with self._init_lock:
            if self._executor is not None:
                return
            executor = self._provided or await create_pool_executor(
                self._dsn or "",
                min_size=self._min_size,
                max_size=self._max_size,
                schema=self._schema,
            )
            try:
                # Before migrations, which would otherwise create the very
                # installation this is trying to warn about.
                await self._check_schema(executor)
                await self._apply_migrations(executor)
                check_protocol_version(await self._read_protocol_version(executor))
            except BaseException:
                # Never leak an executor we created; never close one we were handed.
                if self._provided is None:
                    with contextlib.suppress(Exception):
                        await executor.close()
                raise
            # Publish only a fully-migrated, version-checked executor.
            self._executor = executor
            # Warm the LISTEN connection in the background so the first idle
            # sleep is already wakeable. Fire-and-forget: failure means polling.
            self._listener_ready()
        # Outside the init lock, and after the version check: retention is a
        # background writer, so it must not start against a store this SDK is
        # about to refuse to speak to.
        self._start_retention()

    async def _check_schema(self, session: PgSession) -> None:
        """Refuse a connection pointed somewhere other than the deployment's
        cairnq. The TypeScript SDK applies the same rule; see installations.sql
        for why looking outside the connection's own search_path is the only way
        to see this.

        Two shapes, because ``schema`` means "the schema cairnq's tables live in"
        and cairnq can either arrange that or only check it:

        - ``schema`` given -> assert the connection actually resolves there.
        - ``schema`` omitted -> the dangerous case is being about to create a
          SECOND installation while one already exists elsewhere in this
          database, which is exactly what a mismatched pair of SDKs does.
          Joining an existing installation is fine no matter what else is
          around, so this fires only when this schema has no cairnq and some
          other schema does.

        That narrowness is what keeps it from crying wolf. Two applications each
        running their own cairnq in their own schema are legitimate; the second
        one to be set up trips this once, and naming ``schema`` explicitly —
        which such a deployment should be doing anyway — is both the fix and the
        confirmation.
        """
        rows = await session.query(self._sql["installations"], [])
        row = dict(rows[0]) if rows else {}
        current = row.get("current_schema")
        installations = list(row.get("installations") or [])

        if self._schema is not None:
            if current != self._schema:
                raise SchemaMismatch(
                    f"cairnq is configured for schema {self._schema!r} but this "
                    f"connection resolves to {current!r} — check the connection's "
                    "search_path"
                )
            return
        # A search_path naming nothing that exists: there is no "here" to compare
        # against, and the migrations are about to fail with a clearer message.
        if current is None:
            return
        if not installations or current in installations:
            return

        found = ", ".join(repr(s) for s in installations)
        raise SchemaMismatch(
            f"cairnq tables already exist in schema {found} of this database, but "
            f"this connection resolves to {current!r}, where there are none. "
            "Connecting would create a second, parallel installation that the "
            "other one can never see — an API and a worker split this way agree "
            "about everything except where, and no task crosses. Point this "
            "process at the same schema (schema=..., or options=-c "
            "search_path=... in the DSN), or pass schema= explicitly to confirm "
            "a separate installation is what you meant."
        )

    async def _apply_migrations(self, executor: PgExecutor) -> None:
        await executor.execute(
            "create table if not exists cairnq_migrations "
            "(name text primary key, applied_at_ms bigint not null)"
        )
        for name, sql in load_migrations("postgres"):
            # Check and apply inside one transaction, with the table lock taken
            # up front: two processes cold-starting together would otherwise both
            # see a migration as unapplied and both run it.
            async with executor.transaction() as session:
                await session.execute("lock table cairnq_migrations in exclusive mode")
                already = await session.query(
                    "select 1 from cairnq_migrations where name = $1", [name]
                )
                if not already:
                    await session.execute(sql)  # multi-statement DDL
                    await session.query(
                        "insert into cairnq_migrations (name, applied_at_ms) values "
                        "($1, (extract(epoch from now()) * 1000)::bigint)",
                        [name],
                    )

    async def close(self) -> None:
        self._listener_unavailable = True  # no revival after close
        stop, self._listener = self._listener, None
        if stop is not None:
            with contextlib.suppress(Exception):
                stop()
        if self._listener_connecting is not None:
            self._listener_connecting.cancel()
            with contextlib.suppress(BaseException):
                await self._listener_connecting
        self._release_waiters()
        executor, self._executor = self._executor, None
        # An injected executor belongs to the caller, whose other work would not
        # survive cairnq closing it.
        if executor is not None and self._provided is None:
            await executor.close()

    # ----------------------------------------------------------- wake channel
    async def claim_wake(self, queues: list[str], timeout_ms: int) -> None:
        if not self._listener_ready():
            await super().claim_wake(queues, timeout_ms)
            return
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        while True:
            # Consume every pending notification for our queues; any hit wakes.
            hit = self._pending_queues.intersection(queues)
            if hit:
                self._pending_queues.difference_update(queues)
                return
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return
            # Broadcast wake: another queue's notification loops us back to
            # waiting with the remaining budget instead of waking the worker.
            self._queued_event.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._queued_event.wait(), remaining)

    async def task_done_wake(self, task_id: str, timeout_ms: int) -> None:
        if not self._listener_ready():
            await super().task_done_wake(task_id, timeout_ms)
            return
        event = asyncio.Event()
        waiters = self._done_waiters.setdefault(task_id, set())
        waiters.add(event)
        try:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(event.wait(), timeout_ms / 1000)
        finally:
            waiters.discard(event)
            if not waiters:
                self._done_waiters.pop(task_id, None)

    def _listener_ready(self) -> bool:
        """True once the LISTEN connection is up; starts connecting it
        otherwise (respecting the transient-failure backoff). Callers fall back
        to plain polling until it is ready (or forever, if it can't be
        established) — correctness never depends on it."""
        if self._listener is not None:
            return True
        if (
            not self._listener_unavailable
            and self._listener_connecting is None
            and asyncio.get_running_loop().time() >= self._listener_retry_at
        ):
            self._listener_connecting = asyncio.get_running_loop().create_task(
                self._start_listener()
            )
        return False

    async def _start_listener(self) -> None:
        try:
            executor = self._executor or self._provided
            # Not connected yet: transient by definition — connect() calls back in.
            if executor is None:
                return
            if getattr(executor, "listen", None) is None:
                self._listener_unavailable = True  # this executor will never push
                return
            try:
                stop = await executor.listen(
                    (QUEUED_CHANNEL, DONE_CHANNEL),
                    self._on_notification,
                    # A dropped listener degrades to polling; the next wake
                    # reconnects.
                    self._on_listener_lost,
                )
            except ListenUnavailable:
                # Deterministic (e.g. a transaction-mode pooler): off for good
                # rather than a reconnect loop that cannot succeed.
                self._listener_unavailable = True
                return
            except Exception:
                # Could not establish it — transient. Schedule a backed-off
                # retry; polling covers the gap.
                self._listener_retry_at = (
                    asyncio.get_running_loop().time() + self._listener_backoff_ms / 1000
                )
                self._listener_backoff_ms = min(
                    LISTENER_RETRY_MAX_MS, self._listener_backoff_ms * 2
                )
                return
            if self._listener_unavailable:
                with contextlib.suppress(Exception):
                    stop()  # closed while we were subscribing
                return
            self._listener = stop
            self._listener_backoff_ms = LISTENER_RETRY_MS
        finally:
            self._listener_connecting = None

    def _on_notification(self, channel: str, payload: str | None) -> None:
        if channel == QUEUED_CHANNEL and payload:
            self._pending_queues.add(payload)
            self._queued_event.set()
            self._publish(WatchSignal(reason="queued", queue=payload))
        elif channel == DONE_CHANNEL and payload:
            for event in self._done_waiters.get(payload, ()):
                event.set()
            self._publish(WatchSignal(reason="done", task_id=payload))

    def _publish(self, signal: WatchSignal) -> None:
        """Hand a notification to every watch() subscriber. A raising subscriber
        is its own problem: it must not cost the others their signal, nor take
        down the listener connection that delivered it."""
        for subscriber in list(self._subscribers):
            try:
                subscriber(signal)
            except Exception:
                pass  # deliberately swallowed — see above

    def _on_listener_lost(self) -> None:
        # A dropped listener degrades to polling; the next wake call reconnects.
        self._listener = None
        self._release_waiters()

    def _release_waiters(self) -> None:
        # Release everyone promptly; their fallback poll takes over.
        self._queued_event.set()
        for waiters in list(self._done_waiters.values()):
            for event in waiters:
                event.set()

    async def _read_protocol_version(self, session: PgSession) -> int:
        # Takes an explicit session: during connect the executor is not published
        # yet, so this cannot go through _fetch. The statement binds nothing.
        rows = await session.query(self._sql["protocol_version"], [])
        return int(dict(rows[0])["value"]) if rows else 0

    async def protocol_version(self) -> int:
        await self._ensure()
        assert self._executor is not None
        return await self._read_protocol_version(self._executor)

    async def _ensure(self) -> None:
        if self._executor is None:
            await self.connect()

    # ----------------------------------------------------------- dialect seam
    async def _fetch(self, name: str, params: dict[str, Any]) -> list[Any]:
        await self._ensure()
        assert self._executor is not None
        text, values = to_positional(self._sql[name], params)
        return await self._executor.query(text, values)

    def _bound_fetch(self, session: PgSession) -> Fetch:
        """A Fetch that runs the protocol's statements on one particular session."""

        async def fetch(name: str, params: dict[str, Any]) -> list[Any]:
            text, values = to_positional(self._sql[name], params)
            return await session.query(text, values)

        return fetch

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncIterator[Fetch]:
        await self._ensure()
        assert self._executor is not None
        async with self._executor.transaction() as session:
            yield self._bound_fetch(session)

    @contextlib.asynccontextmanager
    async def _transaction_with_session(self) -> AsyncIterator[tuple[Fetch, Any]]:
        await self._ensure()
        assert self._executor is not None
        async with self._executor.transaction() as session:
            yield self._bound_fetch(session), session

    # ------------------------------------------------------------ push seams
    def _subscribe_push(self, on_signal: Callable[[WatchSignal], None]) -> Callable[[], None]:
        self._subscribers.add(on_signal)
        self._listener_ready()  # an API-side watcher is often the only thing asking
        return lambda: self._subscribers.discard(on_signal)

    def _warm_push(self) -> None:
        if self._subscribers:
            self._listener_ready()
