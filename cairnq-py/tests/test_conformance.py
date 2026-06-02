import json
import re

import pytest

from cairnq import STATUSES, CairnQ
from cairnq._sql import find_protocol_root, load_migrations
from cairnq.models import TERMINAL

from ._runner import Runner

SCENARIO_DIR = find_protocol_root() / "conformance" / "scenarios"
SCENARIOS = sorted(SCENARIO_DIR.glob("*.json"))


@pytest.mark.parametrize("scenario_path", SCENARIOS, ids=lambda p: p.stem)
async def test_conformance(scenario_path, tmp_path):
    data = json.loads(scenario_path.read_text(encoding="utf-8"))
    client = CairnQ.sqlite(str(tmp_path / "t.db"))
    await client.connect()
    try:
        await Runner(client).run(data["steps"])
    finally:
        await client.close()


def test_scenarios_exist():
    assert SCENARIOS, f"no conformance scenarios found in {SCENARIO_DIR}"


def test_status_set_matches_protocol_migration():
    """Pin the SDK status set against the canonical CHECK constraint in the
    migration (the cross-language source of truth). Adding/renaming a status in
    the SQL or the SDK without updating the other fails here; both SDKs run this,
    so they stay aligned with the SQL and transitively with each other."""
    sql = "\n".join(s for _, s in load_migrations())
    match = re.search(r"status\s+in\s*\(([^)]*)\)", sql, re.IGNORECASE)
    assert match, "no status CHECK constraint found in protocol migration"
    sql_statuses = set(re.findall(r"'([^']+)'", match.group(1)))
    assert sql_statuses == set(STATUSES)
    assert set(TERMINAL) <= set(STATUSES)
