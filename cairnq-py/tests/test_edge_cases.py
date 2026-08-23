"""Regression tests for edge cases the library used to mishandle: non-JSON
values crossing the protocol boundary, tasks stranded by unserializable results,
sub-second leases outrun by the heartbeat floor, and silent typo'd list filters.
The TypeScript twin is cairnq-node/test/edge-cases.test.ts.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from cairnq import SerializationError, TaskError, Worker
from cairnq.models import Task

from .conftest import task_row
from cairnq.errors import TaskFailed


async def test_non_finite_floats_are_rejected_at_submit(client):
    # json.dumps would otherwise write bare NaN/Infinity — not JSON — which the
    # TypeScript SDK's JSON.parse throws on, poisoning the row for every
    # cross-language reader.
    with pytest.raises(SerializationError):
        await client.submit("job", {"x": float("nan")})
    with pytest.raises(SerializationError):
        await client.submit("job", {"x": 1.0}, metadata={"y": float("inf")})


async def test_unserializable_result_fails_the_task_promptly(client, db_path):
    # The failure is deterministic, so it must be recorded as a permanent
    # SerializationError on the first attempt — not strand the task `running`
    # until lease expiry redelivers it to fail the same way again.
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20, lease_ms=600)
    runs = 0

    @worker.task("bad-result")
    async def handle(ctx, payload):
        nonlocal runs
        runs += 1
        return {"vals": {1, 2}}  # a set is not JSON-serializable

    async with worker.background():
        task = await client.submit("bad-result", {})
        # Well under the first lease expiry: no redelivery may be involved.
        final = await client.wait(task.id, timeout_ms=500, poll_ms=20)

    assert final.failed
    assert final.error["code"] == "unserializable_result"
    assert runs == 1


async def test_unserializable_task_error_details_still_record_the_failure(client, db_path):
    # A TaskError carrying exotic details must not strand the task: the envelope
    # is stripped to its string fields and the failure is recorded.
    worker = Worker.sqlite(db_path, queues=["default"], poll_interval_ms=20)

    @worker.task("bad-details")
    async def handle(ctx, payload):
        raise TaskError("boom", details={"weird": {1, 2}})

    async with worker.background():
        with pytest.raises(TaskFailed) as excinfo:
            await client.call("bad-details", {}, wait_timeout_ms=3_000, poll_ms=20)

    assert excinfo.value.message == "boom"
    assert excinfo.value.details == {}


async def test_sub_second_lease_is_maintained_by_the_heartbeat(client, db_path):
    # The heartbeat floor used to be 1s, so a lease below that could never be
    # maintained: the worker's own claim loop recovered the "expired" lease and
    # re-ran the task while the first attempt was still going.
    worker = Worker.sqlite(
        db_path, queues=["default"], poll_interval_ms=20, lease_ms=200, concurrency=2
    )
    runs = 0

    @worker.task("slow")
    async def handle(ctx, payload):
        nonlocal runs
        runs += 1
        await asyncio.sleep(0.4)  # outlives two lease periods
        return {"ok": True}

    async with worker.background():
        result = await client.call("slow", {}, wait_timeout_ms=3_000, poll_ms=20)

    assert result == {"ok": True}
    assert runs == 1


async def test_list_rejects_an_unknown_status(client):
    # A typo'd status used to match nothing and return [] indistinguishably
    # from "no such tasks".
    with pytest.raises(ValueError):
        await client.list(status="succeded")  # typo on purpose


async def test_out_of_range_numeric_arguments_are_rejected(client):
    # max_attempts=0 would still run once (claim increments before the check) —
    # a silently different meaning than the number says. The others silently
    # matched nothing or purged nothing.
    with pytest.raises(ValueError):
        await client.submit("job", {}, max_attempts=0)
    with pytest.raises(ValueError):
        await client.submit("job", {}, run_at_delay_ms=-1)
    with pytest.raises(ValueError):
        await client.list(limit=-1)
    with pytest.raises(ValueError):
        await client.list(offset=-1)
    with pytest.raises(ValueError):
        await client.purge(older_than_ms=-1)
    with pytest.raises(ValueError):
        await client.purge(limit=0)


async def test_opaque_types_are_rejected_where_the_twin_needs_a_deny_list(client):
    """The values whose contents JSON cannot carry raise here on their own.

    This is the Python half of the TypeScript twin's opaque-built-in rule (see
    cairnq-node/test/edge-cases.test.ts): `JSON.stringify` empties a Map or a Set
    into `{}` and needs an explicit deny-list to stop it, while this encoder
    already refuses the whole class. Pinned so a future switch to a more lenient
    encoder — one with a `default=` that stringifies the unknown — cannot quietly
    reopen the gap on this side."""
    import datetime
    import decimal
    import uuid

    for value in [
        {1, 2},
        frozenset([1]),
        datetime.datetime(2020, 1, 1),
        decimal.Decimal("1.5"),
        b"bytes",
        uuid.uuid4(),
        object(),
        (i for i in [1]),
    ]:
        with pytest.raises(SerializationError):
            await client.submit("job", {"v": value})
        # Nested and inside a list too — the encoder checks at every node.
        with pytest.raises(SerializationError):
            await client.submit("job", {"deep": {"v": value}})
        with pytest.raises(SerializationError):
            await client.submit("job", {"xs": [value]})


async def test_value_preserving_coercions_are_deliberately_allowed(client):
    """Two conversions are NOT rejected, because JSON has no other form for them
    and both keep the value — only its type is narrowed. Policing them would mean
    walking every value before encoding it, measured at 55-150% on top of the
    encode, paid by every submit. Asserted rather than merely documented so the
    behaviour a caller reads back is pinned; see PROTOCOL.md "JSON"."""
    # A tuple becomes an array, and reads back as a list.
    task = await client.submit("job", {"xs": (1, 2)})
    assert (await client.get(task.id)).payload == {"xs": [1, 2]}
    # A non-str dict key becomes its literal spelling, and reads back under it.
    # One key type per dict: `1` and `True` are the same key to Python itself
    # (equal, same hash), so a mixed literal collapses before json sees it.
    for key, spelled in [(1, "1"), (True, "true"), (None, "null"), (2.5, "2.5")]:
        task = await client.submit("job", {key: "v"})
        assert (await client.get(task.id)).payload == {spelled: "v"}


def test_from_row_does_not_reparse_a_column_the_driver_already_decoded():
    """The Postgres case, reachable without a Postgres.

    asyncpg hands json/jsonb back as text, but a caller may register a decoding
    codec and an injected executor may be over a driver that decodes by default
    (`pg` does). A top-level JSON string then arrives as a bare str —
    indistinguishable from the text wire form, and identical to what the caller
    stored. from_row used to guess from the value and parse it twice: "s3://…"
    raised, and "42" came back as the int 42. The store now tells it which form
    it has (see TaskStore._json_is_text and PostgresStore._detect_json_wire_form).
    """
    # task_row's json_is_text writes every JSON column in the matching form, so
    # the row and the flag it is read with cannot disagree.
    def decoded_row(v):
        return task_row(json_is_text=False, payload=v, result=v)

    def text_row(v):
        return task_row(json_is_text=True, payload=json.dumps(v), result=json.dumps(v))

    for value in ["hello", "42", "true", "null", "[1,2]", "", "s3://img/a.png"]:
        decoded = Task.from_row(decoded_row(value), json_is_text=False)
        assert decoded.payload == value and isinstance(decoded.payload, str)
        assert decoded.result == value
        assert Task.from_row(text_row(value), json_is_text=True).payload == value
    # Both forms agree on the non-string cases too, which is what made the bug
    # invisible: a dict is a dict either way.
    assert Task.from_row(decoded_row({"a": 1}), json_is_text=False).payload == {"a": 1}
    assert Task.from_row(text_row({"a": 1}), json_is_text=True).payload == {"a": 1}
    # A SQL NULL and a decoded JSON null both mean "no value" in either form.
    assert Task.from_row(decoded_row(None), json_is_text=False).result is None
    assert Task.from_row(text_row(None), json_is_text=True).result is None
