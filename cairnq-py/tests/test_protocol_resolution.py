"""Where the SDK loads the protocol from. The failure this pins: a leftover
vendored `cairnq/_protocol/` (written by scripts/vendor-protocol.mjs, gitignored)
used to shadow the repo's cairnq-protocol/ in a dev checkout — so edits to the
canonical SQL were silently not under test, and the conformance scenarios (which
vendoring does not copy) disappeared from the run entirely. CI never sees it: a
fresh checkout has no vendored dir."""

from __future__ import annotations

from pathlib import Path

import pytest

from cairnq._sql import find_protocol_root

REPO_PROTOCOL = Path(__file__).resolve().parents[2] / "cairnq-protocol"


def test_dev_checkout_prefers_repo_protocol_over_vendored():
    if not (REPO_PROTOCOL / "sql").is_dir():
        pytest.skip("not a monorepo checkout (installed package)")
    assert find_protocol_root() == REPO_PROTOCOL


def test_conformance_scenarios_are_reachable_from_the_resolved_root():
    if not (REPO_PROTOCOL / "sql").is_dir():
        pytest.skip("not a monorepo checkout (installed package)")
    scenarios = sorted((find_protocol_root() / "conformance" / "scenarios").glob("*.json"))
    assert scenarios, "resolved protocol root has no conformance scenarios"
