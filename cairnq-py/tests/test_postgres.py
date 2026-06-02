"""Pure-function tests for the named->positional SQL translator. PostgresStore's
runtime behavior (claim races, lock semantics) is covered by the conformance suite
against a real PG instance; this is the one piece of PG plumbing not in SQL."""

import re
from collections import defaultdict

from cairnq._sql import load_statements
from cairnq.store.postgres import to_positional


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
