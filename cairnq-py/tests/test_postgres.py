"""Pure-function tests for the named->positional SQL translator. PostgresStore's
runtime behavior (claim races, lock semantics) is covered by the conformance suite
against a real PG instance; this is the one piece of PG plumbing not in SQL."""

import re
from collections import defaultdict

import pytest

from .conftest import FakeExecutor

from cairnq._sql import load_statements
from cairnq.store.postgres import positional_statement, to_positional


def test_collapses_repeated_name_to_one_slot():
    text, values = to_positional(
        "select * from t where (:status is null or status = :status) and id = :id",
        {"status": "queued", "id": "x"},
    )
    assert text == "select * from t where ($1 is null or status = $1) and id = $2"
    assert values == ["queued", "x"]


def test_strips_comments_so_name_in_comment_is_not_a_param():
    text, values = to_positional(
        "-- extend lease (now + :lease_ms)\nupdate t set x = :x where id = :id",
        {"x": 1, "id": "a"},
    )
    assert "lease_ms" not in text
    assert values == [1, "a"]


def test_leaves_casts_intact():
    text, _ = to_positional("where queue = any(:queues::text[])", {"queues": ["default"]})
    assert text == "where queue = any($1::text[])"


def test_every_postgres_statement_has_no_leftover_named_params():
    leftover = re.compile(r"(?<!:):\w")
    for name, sql in load_statements("postgres").items():
        text, _ = to_positional(sql, defaultdict(lambda: None))
        assert not leftover.search(text), f"{name} has a leftover :named param"


def test_translation_is_memoized_on_the_statement():
    """Statement text is loaded once and never varies, so the rewrite is done
    once. Without this every Postgres query re-scans its SQL with two regexes —
    on the worker's poll loop, for a result that cannot have changed."""
    sql = load_statements("postgres")["claim"]
    assert positional_statement(sql) is positional_statement(sql)


def test_memoized_translation_still_binds_per_call_values():
    """The cache holds the rewrite, not the values — two calls with different
    params must not share a value list."""
    sql = "select * from t where id = :id"
    _, first = to_positional(sql, {"id": "a"})
    _, second = to_positional(sql, {"id": "b"})
    assert first == ["a"] and second == ["b"]


# ------------------------------------------------------- listener resilience
# The LISTEN connection is an accelerator: losing it degrades to polling. These
# pin the retry POLICY, which lives in the store — a failure to even establish
# the subscription is transient and retried with backoff; a server that accepts
# the connection but refuses LISTEN (e.g. a transaction-mode pooler) reports
# ListenUnavailable and the channel is off for good. Which connection actually
# carries it is the executor's business, so these drive the executor seam rather
# than asyncpg.


def _store_listening(listen=None):
    """A store over an executor whose only interesting method is `listen`.

    Omitting `listen` builds an executor that has none, which is a real case:
    not every driver exposes a connection to hold. The listener policy runs off
    `_provided` before connect() publishes an executor, which is what lets these
    skip the migration round trip.
    """
    pytest.importorskip("asyncpg")
    from cairnq.store.postgres import PostgresStore

    return PostgresStore(FakeExecutor(listen=listen))


async def test_listener_connect_failure_is_transient_and_backed_off():
    calls = {"n": 0}

    async def refused(_channels, _on_notify, _on_close):
        calls["n"] += 1
        raise OSError("connection refused")

    store = _store_listening(refused)
    assert store._listener_ready() is False
    await store._listener_connecting
    assert calls["n"] == 1
    assert store._listener_unavailable is False, "a connect failure must not disable for good"
    assert store._listener_retry_at > 0

    # Inside the backoff window: no new attempt is spawned.
    assert store._listener_ready() is False
    assert store._listener_connecting is None and calls["n"] == 1

    # Past the window: it tries again, and the backoff grows.
    first_backoff = store._listener_backoff_ms
    store._listener_retry_at = 0.0
    assert store._listener_ready() is False
    await store._listener_connecting
    assert calls["n"] == 2
    assert store._listener_backoff_ms > first_backoff


async def test_listener_refused_listen_is_permanent():
    from cairnq.store.pg_executor import ListenUnavailable

    calls = {"n": 0}

    async def refusing(_channels, _on_notify, _on_close):
        calls["n"] += 1
        raise ListenUnavailable("LISTEN is not supported here")

    store = _store_listening(refusing)
    assert store._listener_ready() is False
    await store._listener_connecting
    assert store._listener_unavailable is True
    assert store._listener_ready() is False, "permanently off: no further attempts"
    assert store._listener_connecting is None
    assert calls["n"] == 1


async def test_listener_recovers_once_the_server_is_back():
    state = {"up": False}
    stopped = {"n": 0}

    async def connect(_channels, _on_notify, _on_close):
        if not state["up"]:
            raise OSError("connection refused")
        return lambda: stopped.__setitem__("n", stopped["n"] + 1)

    store = _store_listening(connect)
    store._listener_ready()
    await store._listener_connecting  # first attempt fails, schedules a retry

    state["up"] = True
    store._listener_retry_at = 0.0  # fast-forward past the backoff
    store._listener_ready()
    await store._listener_connecting
    assert store._listener is not None
    assert store._listener_ready() is True

    from cairnq.store.postgres import LISTENER_RETRY_MS

    assert store._listener_backoff_ms == LISTENER_RETRY_MS, "backoff resets on success"


async def test_an_executor_that_cannot_listen_is_off_for_good():
    # Not every driver exposes a dedicated connection; the contract says such an
    # executor costs latency, never correctness.
    store = _store_listening()
    assert store._listener_ready() is False
    await store._listener_connecting
    assert store._listener_unavailable is True
