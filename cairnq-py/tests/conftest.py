import pytest_asyncio

from cairnq import CairnQ


@pytest_asyncio.fixture
async def db_path(tmp_path):
    return str(tmp_path / "tasks.db")


@pytest_asyncio.fixture
async def client(db_path):
    c = CairnQ.sqlite(db_path)
    await c.connect()
    try:
        yield c
    finally:
        await c.close()


# ------------------------------------------------------------- fake executor
# One PgExecutor stub, shared by every test that needs a PostgresStore without a
# Postgres. Kept here rather than per file because nothing type-checks a Protocol
# implementation in Python: a second copy would mean a change to PgExecutor
# breaks only whichever file happens to exercise the new method.


class FakeSession:
    """Answers the statements PostgresStore runs on the way up, records the rest.

    `installations` is the shape installations.sql produces — one row per schema
    holding cairnq, and one all-null-schema row when there are none.
    """

    def __init__(
        self,
        calls: list,
        *,
        complete_matches: bool = True,
        completed_row: dict | None = None,
        current_schema: str | None = None,
        installations: list[str] | None = None,
    ):
        self.calls = calls
        self._complete_matches = complete_matches
        self._completed_row = completed_row
        found = installations or []
        self._installations = [
            {"current_schema": current_schema, "schema": s} for s in found
        ] or [{"current_schema": current_schema, "schema": None}]

    async def query(self, text: str, values=()) -> list:
        if "current_schema()" in text:
            return self._installations
        if "protocol_version" in text and "select" in text:
            return [{"value": "1"}]
        if "::jsonb as probe" in text:
            # This fake hands rows back with their JSON columns already decoded
            # (see the dict-valued `payload` in the test rows), so it answers the
            # wire-form probe the way a decoding driver would: the value without
            # its quotes. Stated rather than left to the fall-through, so the
            # harness says which driver it is emulating.
            return [{"probe": "cairnq"}]
        if "update cairnq_tasks" in text and "succeeded" in text:
            self.calls.append("complete")
            return [self._completed_row] if self._complete_matches else []
        self.calls.append("query")
        return []

    async def execute(self, sql: str) -> None:
        self.calls.append("execute")


class FakeExecutor:
    """A PgExecutor over FakeSession. `listen` is absent unless one is passed —
    an executor without it is a real case the store has to handle."""

    def __init__(self, listen=None, **session_kwargs):
        self.calls: list = []
        self.rolled_back = False
        self.session = FakeSession(self.calls, **session_kwargs)
        if listen is not None:
            self.listen = listen

    async def query(self, text: str, values=()) -> list:
        return await self.session.query(text, values)

    async def execute(self, sql: str) -> None:
        await self.session.execute(sql)

    def transaction(self):
        executor = self

        class _Txn:
            async def __aenter__(self):
                executor.calls.append("BEGIN")
                return executor.session

            async def __aexit__(self, exc_type, exc, tb):
                if exc_type is None:
                    executor.calls.append("COMMIT")
                else:
                    executor.rolled_back = True
                    executor.calls.append("ROLLBACK")
                return False

        return _Txn()

    async def close(self) -> None:
        pass


def task_row(json_is_text: bool = True, **overrides) -> dict:
    """A cairnq_tasks row, for the tests that exercise row mapping directly.

    Here rather than per file for the reason FakeExecutor is: the shape is a fact
    about the SCHEMA, so a copy per file means a migration adding a column has
    several places to reach, and only whichever file happens to exercise it
    notices when one is missed.

    ``json_is_text`` picks which wire form the JSON columns are written in, so a
    caller can hand the result to ``Task.from_row`` with the matching flag and
    have the two agree by construction."""
    now = 1_700_000_000_000
    empty = "{}" if json_is_text else {}
    return {
        "id": "t1", "name": "render", "queue": "default", "status": "succeeded",
        "payload": empty, "metadata": empty, "result": None, "error": None,
        "progress": None, "message": None, "attempt": 1, "max_attempts": 3,
        "priority": 0, "worker_id": "w1", "lease_until_ms": None,
        "run_at_ms": now, "cancel_requested_at_ms": None, "parent_id": None,
        "root_id": None, "correlation_id": None, "created_at_ms": now,
        "updated_at_ms": now, "completed_at_ms": now,
        **overrides,
    }
