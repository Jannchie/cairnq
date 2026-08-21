"""The split-brain guard: two ends of one deployment pointed at different schemas.

`search_path` is out-of-band configuration, so two processes given the same DSN
can land in different schemas. Every migration is `create table if not exists`,
so the odd one out does not fail — it builds a second, empty installation, and
its protocol version check passes against the `cairnq_meta` it just created. The
API's tasks are then invisible to the worker forever, with nothing logged.

These drive `_check_schema` directly against a stub connection: what it decides
is the whole behavior, and the decision needs no database. The TypeScript SDK
applies the same rule, tested the same way.
"""

from __future__ import annotations

import pytest

from cairnq.errors import SchemaMismatch
from cairnq.store.postgres import PostgresStore

pytest.importorskip("asyncpg")


class StubSession:
    """Answers installations.sql and nothing else."""

    def __init__(self, current: str | None, installations: list[str]):
        self._row = {"current_schema": current, "installations": installations}

    async def query(self, _text: str, _values):
        return [self._row]

    async def execute(self, _sql: str) -> None:
        pass


def store(schema: str | None = None) -> PostgresStore:
    return PostgresStore("postgresql://unused/db", schema=schema)


async def test_refuses_to_build_a_second_installation_beside_an_existing_one():
    # The reported failure, in the direction that actually bites: the other SDK
    # migrated into `cairnq`, this one defaults to `public`.
    with pytest.raises(SchemaMismatch) as e:
        await store()._check_schema(StubSession("public", ["cairnq"]))
    assert "'cairnq'" in str(e.value) and "'public'" in str(e.value)


async def test_quiet_when_joining_the_installation_that_exists():
    await store()._check_schema(StubSession("cairnq", ["cairnq"]))


async def test_quiet_on_a_database_with_no_cairnq_yet():
    await store()._check_schema(StubSession("public", []))


async def test_an_explicit_schema_allows_two_deployments_in_one_database():
    # Saying it is both the fix and the confirmation — which is what the error
    # in the ambiguous case tells you to do.
    await store("app_b")._check_schema(StubSession("app_b", ["app_a"]))


async def test_an_explicit_schema_is_checked_against_where_the_connection_landed():
    with pytest.raises(SchemaMismatch, match="configured for schema 'cairnq'"):
        await store("cairnq")._check_schema(StubSession("public", ["public"]))


async def test_quiet_when_the_search_path_resolves_to_nothing():
    # Nothing to compare against, and the migrations are about to fail with a
    # message that names the real problem.
    await store()._check_schema(StubSession(None, ["cairnq"]))


def test_schema_must_be_a_plain_identifier():
    # It is quoted into `create schema if not exists "..."`, so a name that could
    # close that quote could rewrite the statement.
    for bad in ['pub"lic', "a; drop table cairnq_tasks", "1st", "with space", ""]:
        with pytest.raises(ValueError, match="plain identifier"):
            PostgresStore("postgresql://unused/db", schema=bad)
