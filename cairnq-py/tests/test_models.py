"""Which columns Task.from_row normalizes is a fact about the SCHEMA, but it is
kept as a hand-written list — and so is the TypeScript SDK's, separately.

Adding a bigint column to the migration and forgetting one of those lists is a
silent failure: the field simply arrives in whatever wire form the driver chose,
and the conformance suite compares shared behavior, so it stays green whether one
SDK misses it or both do. The lists cost nothing at runtime; this is what keeps
them honest. Mirrors the same gate in optional-drivers.test.ts.
"""

from __future__ import annotations

import re

from cairnq._sql import find_protocol_root
from cairnq.models import _JSON_COLUMNS, _MS_COLUMNS


def _columns_of_type(sql_type: str) -> list[str]:
    ddl = (find_protocol_root() / "migrations" / "postgres" / "0001_init.sql").read_text()
    # Scoped to cairnq_tasks: cairnq_task_keys has *_ms columns of its own, and
    # they are not part of a Task row.
    table = re.search(
        r"create table if not exists cairnq_tasks \((.*?)\n\);", ddl, re.S
    )
    assert table, "cannot find the cairnq_tasks DDL"
    return re.findall(rf"^\s*(\w+)\s+{sql_type}\b", table.group(1), re.M)


def test_ms_columns_cover_every_bigint_column():
    assert sorted(_MS_COLUMNS) == sorted(_columns_of_type("bigint"))


def test_json_columns_cover_every_jsonb_column():
    assert sorted(_JSON_COLUMNS) == sorted(_columns_of_type("jsonb"))
