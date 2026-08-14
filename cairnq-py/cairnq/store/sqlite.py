"""SQLiteStore — the SQLite dialect of the shared cairnq-protocol SQL.

Everything protocol-shaped lives in TaskStore; this file is only what SQLite does
differently: one aiosqlite connection guarded by an asyncio.Lock, BEGIN IMMEDIATE
transactions, a read-only probe in front of the write lock, and time supplied by
the SDK (`:now_ms`) rather than by the database.

Concurrency model: SQLite is a single writer anyway, and the lock prevents
multi-statement transactions (submit-with-key, recover+claim) from interleaving on
the shared connection. It is only ever held for short DB work — never while a task
handler runs.

Cross-process contention is absorbed by busy_timeout, which is right here and wrong
in the TypeScript twin: aiosqlite waits on its own connection thread, so the event
loop never stalls, while better-sqlite3 would block the only thread there is (that
SDK sets busy_timeout = 0 and retries instead). Restoring symmetry here would move
the wait onto the event loop for no gain, and this seam is a context manager rather
than a callback, so it could not re-run an attempt anyway."""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import sqlite3
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiosqlite

from .._ids import now_ms
from .._sql import load_migrations, load_statements
from .base import COMMENT, Fetch, TaskStore, check_protocol_version, statement_params


def _split_script(script: str) -> list[str]:
    """Split a migration into single statements.

    `executescript` would be the obvious tool, but it COMMITs before it runs —
    which would silently end the transaction the migration is supposed to be
    applied in. `sqlite3.complete_statement` gives the same split without the
    hidden commit, and understands strings and comments so a `;` inside either
    doesn't cut a statement in half."""
    statements: list[str] = []
    buffer = ""
    for line in script.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statements.append(buffer.strip())
            buffer = ""
    if buffer.strip():
        statements.append(buffer.strip())
    return statements


_WAL_RETRY_DELAY_S = 0.05
_WAL_RETRY_BUDGET_S = 5.0

# How often a live connection revisits its planner statistics.
#
# Bounds how long the planner can work from a stale table shape; a minute is
# arbitrary but small next to the days a worker holds its connection. It does not
# set how often an ANALYZE actually runs — SQLite decides that itself, and only once
# the table has diverged from its statistics by 10x, so a shorter interval costs
# more no-ops (a few microseconds each) rather than more analyzing.
_STATS_REFRESH_INTERVAL_S = 60.0


async def _has_statistics(conn: aiosqlite.Connection) -> bool:
    """Whether cairnq_tasks has been analyzed at all.

    Two steps because sqlite_stat1 does not exist until something runs ANALYZE, and
    querying a missing table is an error rather than an empty result."""
    cur = await conn.execute(
        "select 1 from sqlite_master where type = 'table' and name = 'sqlite_stat1'"
    )
    table = await cur.fetchone()
    await cur.close()
    if table is None:
        return False
    cur = await conn.execute("select 1 from sqlite_stat1 where tbl = 'cairnq_tasks'")
    row = await cur.fetchone()
    await cur.close()
    return row is not None


async def _refresh_statistics(conn: aiosqlite.Connection) -> None:
    """Bring cairnq_tasks' statistics up to date, cheaply enough to call on a timer.

    Without them the planner misreads `status = 'running'` as a large fraction of the
    table and passes over the partial cairnq_tasks_lease_idx that lease recovery is
    indexed for.

    The explicit bootstrap is not redundant with `PRAGMA optimize`. Before SQLite
    3.46 the pragma skips a table that has no sqlite_stat1 entry entirely — no mask
    changes that, verified on 3.45.1 — so on those builds it can never produce the
    *first* statistics, and the index stays unused for the life of the database. This
    SDK links whatever SQLite the interpreter was built against, and distro Pythons
    ship exactly those builds (Ubuntu 24.04: 3.45.1), so without the bootstrap the
    partial index would be dead for most Python deployments while working in
    TypeScript, which bundles a newer SQLite.

    Once an entry exists, every version's pragma applies its own growth heuristic,
    which is the part worth deferring to: it is a few microseconds when there is
    nothing to do, where a bare ANALYZE would rescan the table every time."""
    if await _has_statistics(conn):
        await conn.execute("pragma optimize")
    else:
        # Scoped to the one table whose shape the planner gets wrong; the key and
        # meta tables are read by primary key, where statistics change nothing. A
        # database this one shares with the caller's own tables is left alone.
        await conn.execute("ANALYZE cairnq_tasks")


def _is_memory(path: str) -> bool:
    """Whether this path names an in-memory database rather than a file."""
    return path == ":memory:" or "mode=memory" in path


async def _enable_wal(conn: aiosqlite.Connection) -> None:
    """Put the database in WAL mode, waiting out a concurrent cold start.

    journal_mode is a persistent property of the file, so only the first
    connection to a new database actually switches it — and that switch needs an
    exclusive lock. `busy_timeout` does not cover it: SQLite returns SQLITE_BUSY
    for a journal_mode change rather than invoking the busy handler, so several
    processes opening the same new database at once would otherwise get an
    instant "database is locked". Retry briefly instead; the window is only as
    long as one other opener's switch.

    Callers must skip in-memory databases: those report journal_mode = "memory"
    and can never be WAL, so waiting for one is waiting for something that will
    not happen.
    """
    deadline = asyncio.get_running_loop().time() + _WAL_RETRY_BUDGET_S
    while True:
        try:
            cur = await conn.execute("pragma journal_mode = WAL")
            row = await cur.fetchone()
            await cur.close()
            if row is not None and str(row[0]).lower() == "wal":
                return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc) and "busy" not in str(exc):
                raise
        if asyncio.get_running_loop().time() >= deadline:
            raise sqlite3.OperationalError(
                "could not switch the database to WAL mode: it stayed locked by "
                "another connection"
            )
        await asyncio.sleep(_WAL_RETRY_DELAY_S)


def _is_write_statement(sql: str) -> bool:
    """Whether this statement writes, and so belongs in a group commit.

    Read from the SQL rather than from a list of statement names, which would be a
    second place to remember when the protocol gains a statement. Every protocol
    statement is a single top-level select, insert, update or delete.

    Reads must stay out of the batch: claimable_probe exists precisely so an idle
    worker never takes SQLite's write lock, and a BEGIN IMMEDIATE around it would
    hand that back."""
    return not re.match(r"\s*select", re.sub(COMMENT, "", sql), re.IGNORECASE)


@dataclass
class _Pending:
    """One write waiting for its turn on the shared connection.

    The rows go back to the caller that asked for them, so a batch resolves each
    member with its own result rather than a merged one."""

    name: str
    params: dict[str, Any]
    future: asyncio.Future[list[aiosqlite.Row]]

    def resolve(self, rows: list[aiosqlite.Row]) -> None:
        # A caller whose await was cancelled leaves a done future behind, and its
        # write still ran — there is simply nobody left to hand the rows to.
        if not self.future.done():
            self.future.set_result(rows)

    def reject(self, exc: BaseException) -> None:
        if not self.future.done():
            self.future.set_exception(exc)


class SQLiteStore(TaskStore):
    def __init__(self, path: str, *, busy_timeout_ms: int = 5_000):
        self._path = path
        self._busy_timeout_ms = busy_timeout_ms
        self._conn: aiosqlite.Connection | None = None
        self._sql = load_statements("sqlite")
        self._lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()
        # When this connection may next revisit its planner statistics. Set on
        # connect, which is also where the first refresh runs.
        self._next_stats_refresh_at = 0.0
        # Group commit: which statements write, and the writes waiting to share a
        # transaction. See _flush.
        self._writes = {name: _is_write_statement(sql) for name, sql in self._sql.items()}
        self._pending: list[_Pending] = []
        self._flushing = False
        # Held so the fire-and-forget flusher cannot be garbage collected mid-batch.
        self._flusher: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------ setup
    async def connect(self) -> None:
        if self._conn is not None:
            return
        async with self._init_lock:
            if self._conn is not None:
                return
            memory = _is_memory(self._path)
            if not memory:
                Path(self._path).parent.mkdir(parents=True, exist_ok=True)
            conn = await aiosqlite.connect(self._path, isolation_level=None)
            conn.row_factory = aiosqlite.Row
            # busy_timeout first, so every later statement waits out contention
            # instead of failing instantly.
            await conn.execute(f"pragma busy_timeout = {self._busy_timeout_ms}")
            # WAL exists so several processes can share one file. An in-memory
            # database is private to this connection, so there is nothing to
            # share and nothing to wait for.
            if not memory:
                await _enable_wal(conn)
            await conn.execute("pragma foreign_keys = ON")
            await self._apply_migrations(conn)
            # Give the query planner statistics: without sqlite_stat1 it misreads
            # `status = 'running'` as a large fraction of the table and passes over
            # the partial cairnq_tasks_lease_idx that lease recovery is indexed for.
            # See _refresh_statistics; repeated on a timer from here on (see
            # _maybe_refresh_statistics).
            try:
                await _refresh_statistics(conn)
            except sqlite3.OperationalError:
                # Statistics are an optimization, never correctness, so losing them
                # to a concurrent writer must not fail the connect — the next one
                # gets another chance.
                pass
            self._next_stats_refresh_at = (
                asyncio.get_running_loop().time() + _STATS_REFRESH_INTERVAL_S
            )
            self._conn = conn
            check_protocol_version(await self.protocol_version())
        # Outside the init lock, and after the version check: retention is a
        # background writer, so it must not start against a store this SDK is
        # about to refuse to speak to.
        self._start_retention()

    async def _apply_migrations(self, conn: aiosqlite.Connection) -> None:
        await conn.execute(
            "create table if not exists cairnq_migrations "
            "(name text primary key, applied_at_ms integer not null)"
        )
        for name, sql in load_migrations("sqlite"):
            # Check and apply under one write lock. Two processes cold-starting on
            # a shared database would otherwise both see a migration as unapplied
            # and both run it — harmless for the idempotent ones, not for a future
            # ALTER. BEGIN IMMEDIATE serializes them; the loser sees it applied.
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur = await conn.execute(
                    "select 1 from cairnq_migrations where name = ?", (name,)
                )
                already = await cur.fetchone()
                await cur.close()
                if already is None:
                    for statement in _split_script(sql):
                        await conn.execute(statement)
                    await conn.execute(
                        "insert into cairnq_migrations (name, applied_at_ms) values (?, ?)",
                        (name, now_ms()),
                    )
            except BaseException:
                # Shielded and BaseException-suppressed: a cancellation landing
                # during the rollback must not abandon the shared connection
                # inside an open write transaction.
                with contextlib.suppress(BaseException):
                    await asyncio.shield(conn.execute("ROLLBACK"))
                raise
            else:
                await conn.execute("COMMIT")

    async def close(self) -> None:
        # Let an in-flight group commit finish first: it is holding writes whose
        # callers are still awaiting them, and closing the connection underneath it
        # would turn those into connection errors for work that was about to land.
        flusher, self._flusher = self._flusher, None
        if flusher is not None:
            with contextlib.suppress(BaseException):
                await flusher
        if self._conn is not None:
            conn, self._conn = self._conn, None
            await conn.close()

    async def _ensure(self) -> aiosqlite.Connection:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None
        return self._conn

    async def protocol_version(self) -> int:
        rows = await self._fetch("protocol_version", {})
        return int(rows[0]["value"]) if rows else 0

    # ----------------------------------------------------------- dialect seam
    def _bind(self, sql: str, params: dict[str, Any]) -> dict[str, Any]:
        """Adapt the dialect-neutral parameters to what this statement binds.

        SQLite statements carry no DB clock, so every absolute `*_ms` is derived
        here from one `now`, and booleans cross as 0/1. The result is narrowed to
        the names the SQL actually uses, which is what makes it safe for a caller
        to pass one superset of parameters for both dialects.

        Each derivation writes a name Postgres does not use (`lease_until_ms` from
        `lease_ms`, and so on), so a statement binds one or the other, never both.
        """
        now = now_ms()
        bound: dict[str, Any] = {}
        for name in statement_params(sql):
            if name == "now_ms":
                bound[name] = now
            elif name == "lease_until_ms":
                bound[name] = now + params["lease_ms"]
            elif name == "run_at_ms":
                bound[name] = now + params["delay_ms"]
            elif name == "before_ms":
                bound[name] = now - params["older_than_ms"]
            elif name in ("queues", "ids"):
                # json_each needs a JSON array. Postgres binds the list itself as
                # text[], so only this dialect encodes.
                bound[name] = json.dumps(list(params[name]))
            elif name == "names":
                # json_each needs a JSON array; null stays null so the SQL's
                # `:names is null` arm means "no filter".
                value = params["names"]
                bound[name] = None if value is None else json.dumps(list(value))
            elif name in ("retryable", "reset_attempt"):
                bound[name] = 1 if params[name] else 0
            else:
                bound[name] = params[name]
        return bound

    async def _run(self, name: str, params: dict[str, Any]) -> list[aiosqlite.Row]:
        sql = self._sql[name]
        cur = await self._conn.execute(sql, self._bind(sql, params))
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def _maybe_refresh_statistics(self) -> None:
        """Revisit this connection's planner statistics, at most once per
        _STATS_REFRESH_INTERVAL_S.

        A connection lives for days, and its statements were prepared against
        whatever the table looked like when it opened — a worker started against an
        empty database plans as if it were still empty however large the backlog
        grows. Cached statements do pick the refreshed plans up: ANALYZE bumps the
        schema cookie, so SQLite silently re-prepares them on next use. That is what
        makes this worth doing rather than a restart-only concern.

        Must be called with the store lock free — it takes it."""
        loop = asyncio.get_running_loop()
        if loop.time() < self._next_stats_refresh_at:
            return
        # Claim the slot before running, not after: otherwise a burst of concurrent
        # operations all see it due and queue an ANALYZE apiece.
        self._next_stats_refresh_at = loop.time() + _STATS_REFRESH_INTERVAL_S
        async with self._lock:
            # Statistics are best-effort; a writer this could not wait out costs
            # nothing but the interval until the next attempt.
            with contextlib.suppress(sqlite3.OperationalError):
                await _refresh_statistics(self._conn)

    async def _flush(self) -> None:
        """Group commit: one transaction for every write already waiting on the lock.

        A write costs microseconds to execute and a WAL commit to durably land, so N
        concurrent writes spend nearly all their time on N commits they could have
        shared. Measured at 200 finalizes: 1414µs each one-transaction-apiece against
        302µs each in one transaction (`bench/sweep` sweep B).

        Nothing waits to form a batch — a flusher takes whatever arrived while the
        previous one held the lock — so this trades no latency for the throughput.
        What it does trade is atomicity: two callers' writes now land together or not
        at all. Under at-least-once that is not observable (a lost batch is a
        redelivery), and it is why every member is resolved only after COMMIT.

        Called with the store lock held."""
        # One writer waiting is the uncontended case, and it stays exactly as cheap
        # as before: wrapping a single statement in BEGIN/COMMIT would add two
        # statements to every write on an idle store.
        if len(self._pending) == 1:
            only = self._pending.pop()
            try:
                only.resolve(await self._run(only.name, only.params))
            except BaseException as exc:  # noqa: BLE001 - handed to its own waiter
                only.reject(exc)
            return

        batch: list[_Pending] = []
        out: list[tuple[list[aiosqlite.Row] | None, BaseException | None]] = []
        try:
            # BEGIN before taking the batch, so a failure here still finds it in
            # _pending — see the handler below.
            await self._conn.execute("BEGIN IMMEDIATE")
            batch, self._pending = self._pending, []
            for write in batch:
                try:
                    out.append((await self._run(write.name, write.params), None))
                except BaseException as exc:  # noqa: BLE001 - may belong to one waiter
                    # A statement error aborts that statement, not the transaction,
                    # so the rest of the batch is still good and this one waiter
                    # carries the error. If SQLite tore the transaction down instead,
                    # nothing in it survived and every member has to hear about it.
                    if not self._conn.in_transaction:
                        raise
                    out.append((None, exc))
            await self._conn.execute("COMMIT")
        except BaseException as exc:  # noqa: BLE001 - fanned out to the whole batch
            if self._conn.in_transaction:
                # Shielded and BaseException-suppressed for the same reason as
                # _transaction: a cancellation here must not leave the shared
                # connection inside an open write transaction.
                with contextlib.suppress(BaseException):
                    await asyncio.shield(self._conn.execute("ROLLBACK"))
            # BEGIN itself failed, so the batch was never taken and is still queued.
            if not batch:
                batch, self._pending = self._pending, []
            for write in batch:
                write.reject(exc)
            return
        # Only now: before COMMIT a rollback could still take the write back, and a
        # caller holding its row would have observed a write that never happened.
        for write, (rows, failure) in zip(batch, out, strict=True):
            if failure is not None:
                write.reject(failure)
            else:
                write.resolve(rows or [])

    async def _flush_loop(self) -> None:
        """Drain `_pending`, one transaction per turn on the lock.

        Loops rather than re-arming per batch: a caller that awaits its writes one at
        a time resumes and issues the next one before the flusher gets its turn back,
        so re-arming would cost that write an extra trip through the lock queue.

        The exit is safe because the last `_pending` check and clearing the flag
        happen without an await between them: a write that arrives before it keeps
        the loop going, and one that arrives after sees the flag down and starts a new
        flusher."""
        try:
            while self._pending:
                async with self._lock:
                    await self._flush()
        except BaseException as exc:  # noqa: BLE001 - nothing above may be silent
            # _flush delivers every outcome to its own waiter, so reaching here means
            # something outside it failed — losing the connection under a close, say.
            # A fire-and-forget task must not swallow that: the writes still queued
            # would wait forever for a flusher that is already gone.
            stranded, self._pending = self._pending, []
            for write in stranded:
                write.reject(exc)
        finally:
            self._flushing = False

    def _schedule_flush(self) -> None:
        if self._flushing:
            return
        self._flushing = True
        self._flusher = asyncio.ensure_future(self._flush_loop())

    async def _fetch(self, name: str, params: dict[str, Any]) -> list[aiosqlite.Row]:
        await self._ensure()
        await self._maybe_refresh_statistics()
        # Reads keep their own turn on the lock — see _is_write_statement.
        if not self._writes[name]:
            async with self._lock:
                return await self._run(name, params)
        pending = _Pending(name, params, asyncio.get_running_loop().create_future())
        self._pending.append(pending)
        self._schedule_flush()
        return await pending.future

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncIterator[Fetch]:
        """BEGIN IMMEDIATE … COMMIT on the shared connection, rolling back on any
        error. The store lock is held for the whole transaction, so no other
        operation can slip a statement into it."""
        await self._ensure()
        await self._maybe_refresh_statistics()
        async with self._lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._run
            except BaseException:
                # Shielded and BaseException-suppressed: a cancellation landing
                # during the rollback must not abandon the shared connection
                # inside an open write transaction — every later operation
                # would then run inside it, holding SQLite's write lock.
                with contextlib.suppress(BaseException):
                    await asyncio.shield(self._conn.execute("ROLLBACK"))
                raise
            else:
                await self._conn.execute("COMMIT")

    async def _has_claimable_work(self, params: dict[str, Any]) -> bool:
        # Read-only probe first: an idle worker never takes SQLite's single write
        # lock, so idle workers don't serialize against each other.
        rows = await self._fetch("claimable_probe", params)
        return bool(rows and rows[0]["has_work"])
