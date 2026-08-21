"""The seam between PostgresStore and whatever actually talks to Postgres.

PostgresStore owns the *dialect* — the protocol's named-parameter SQL rewritten
to ``$n``, the migration ledger, the LISTEN policy. It does not own the
*connection*. Applications that already run a Postgres driver (SQLAlchemy, a pool
they sized themselves, an asyncpg pool shared with the rest of the service) pass
their own executor and cairnq joins that session instead of opening a second one,
which is what makes a task's settlement commit in the same transaction as the
rows the task produced.

Implementing one is small — see ``pool_executor`` for the reference
implementation over asyncpg. An adapter passes rows through as its driver
produced them: cairnq normalizes both column types the drivers disagree about
(jsonb decoded or not, int8 as text or int) in ``Task.from_row``, so no adapter
has to reconfigure its driver — and none has to change how the application's OWN
columns come back in order to satisfy cairnq.

Mirrors ``PgExecutor`` in the TypeScript SDK, method for method.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol, runtime_checkable


class ListenUnavailable(Exception):
    """LISTEN will not work on this connection, and retrying cannot change that
    — a transaction-mode pooler, say. See ``PgExecutor.listen``."""


@runtime_checkable
class PgSession(Protocol):
    """Somewhere statements can run.

    The same shape whether it is a pool (each call on some connection) or one
    transaction's dedicated connection — the store's statements do not care, and
    this is what lets ``transaction`` hand the same interface to its caller.
    """

    async def query(self, text: str, values: Sequence[Any]) -> list[Any]:
        """One parameterised statement, ``$1``-style. ``values`` is positional
        and may legitimately contain None — a null parameter is "this filter is
        off" in several protocol statements, not a missing argument. Rows may be
        anything ``dict()`` accepts (asyncpg's Record, a mapping, ...)."""
        ...

    async def execute(self, sql: str) -> None:
        """Parameterless SQL that may hold several statements, for migration DDL.

        Separate from ``query`` because it must go over the simple query
        protocol: the extended protocol a parameterised call uses accepts only
        one statement, and every migration is a script."""
        ...


@runtime_checkable
class PgExecutor(PgSession, Protocol):
    """A session that can also open transactions, listen, and be shut down."""

    def transaction(self) -> AbstractAsyncContextManager[PgSession]:
        """Yield a session inside one transaction on one dedicated connection,
        committing on a clean exit and rolling back on an exception.

        The store relies on both halves: a claim that cannot see its own
        ``recover_leases`` is a double-dispatch, and a keyed submit that commits
        half way poisons the key."""
        ...

    async def listen(
        self,
        channels: Sequence[str],
        on_notify: Callable[[str, str | None], None],
        on_close: Callable[[], None],
    ) -> Callable[[], None]:
        """Subscribe a dedicated connection to ``channels``.

        Optional: an executor that omits it (or a Postgres that refuses LISTEN)
        costs latency, never correctness — the store falls back to plain polling,
        which is the contract PROTOCOL.md gives for push wakeups. Returns a
        callable that stops listening; ``on_close`` reports a connection that
        dropped on its own, so the store can degrade and retry.

        Raise ``ListenUnavailable`` when this Postgres will never accept LISTEN.
        Any other exception is read as transient and retried with backoff, so a
        permanent condition raised as a plain error becomes a reconnect loop that
        cannot succeed."""
        ...

    async def close(self) -> None:
        """Release this executor's resources. Called by ``PostgresStore.close()``
        ONLY for an executor the store created itself: an injected one belongs to
        the caller, whose other work would not survive cairnq closing it."""
        ...
