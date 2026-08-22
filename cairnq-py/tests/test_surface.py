"""The parity gate. See cairnq-protocol/surface.json for why it exists: the
conformance suite compares SHARED behavior, so a capability only one SDK has
nothing to disagree with, and drift between the two is invisible to it.

This asserts both directions against the shared declaration. The second — every
member here is declared there — is the load-bearing one: it makes a one-sided
addition fail on the side that added it, while it is being added.

Mirrors `surface.test.ts`.
"""

from __future__ import annotations

import inspect
import json

import pytest

from cairnq import CairnQ, Worker
from cairnq._sql import find_protocol_root
from cairnq.context import TaskContext

SURFACE = json.loads((find_protocol_root() / "surface.json").read_text())
CLASSES = {"CairnQ": CairnQ, "Worker": Worker, "TaskContext": TaskContext}


def members_of(cls: type) -> set[str]:
    """Every member this class exposes, by Python's own convention: a leading
    underscore is private, and everything else is API."""
    return {name for name, _ in inspect.getmembers(cls) if not name.startswith("_")}


def names(entries: list) -> list[str]:
    return [e["member"] for e in entries]


@pytest.mark.parametrize("cls_name", sorted(CLASSES))
def test_has_every_shared_member(cls_name: str):
    declared = SURFACE["classes"][cls_name]
    missing = sorted(set(declared["shared"]) - members_of(CLASSES[cls_name]))
    assert not missing, (
        f"declared in surface.json but absent from cairnq-py: {missing}"
    )


@pytest.mark.parametrize("cls_name", sorted(CLASSES))
def test_exemptions_still_describe_reality(cls_name: str):
    declared = SURFACE["classes"][cls_name]
    actual = members_of(CLASSES[cls_name])
    assert not [m for m in names(declared["only_py"]) if m not in actual]
    # A member listed as Node-only that turns up here means the asymmetry was
    # closed and the exemption outlived its reason.
    leaked = sorted(m for m in names(declared["only_node"]) if m in actual)
    assert not leaked, (
        f"exists in cairnq-py but surface.json still calls it Node-only: {leaked}"
    )


@pytest.mark.parametrize("cls_name", sorted(CLASSES))
def test_declares_every_member_it_exposes(cls_name: str):
    declared = SURFACE["classes"][cls_name]
    # `internal` is NOT honored here. It exists only because TypeScript's
    # `private` is erased at runtime, so the Node gate cannot tell a private
    # method from a public one on its own. Python marks its own with a leading
    # underscore, which members_of already drops — and honoring the list anyway
    # would turn it into an escape hatch that silences BOTH gates, which is
    # exactly the load-bearing direction surface.json exists to keep loud.
    known = set(declared["shared"]) | set(names(declared["only_py"]))
    undeclared = sorted(members_of(CLASSES[cls_name]) - known)
    assert not undeclared, (
        "add these to cairnq-protocol/surface.json — to `shared` (and implement "
        "them in cairnq-node), to `only_py` with a reason, or rename them with a "
        f"leading underscore if they are internal: {undeclared}"
    )


# --- the module gate ---
# `classes` above covers the members of the three classes a caller drives; this
# covers what the package exports at all. The failure mode it exists for is the
# same one — a capability shipped on one side only — one level up, where the
# class gate cannot see it.

MODULES = SURFACE["modules"]


def exported() -> set[str]:
    """Python's declared public surface: `__all__`. Not `dir(cairnq)`, which also
    holds the names __init__ imported to build those (PackageNotFoundError, the
    submodules) — `__all__` is the list the package is asserting, and asserting
    against the assertion is what makes forgetting to update it visible."""
    import cairnq

    return set(cairnq.__all__)


def test_exports_everything_shared():
    missing = sorted(set(MODULES["shared"]) - exported())
    assert not missing, f"declared in surface.json but not exported by cairnq-py: {missing}"


def test_module_exemptions_still_describe_reality():
    actual = exported()
    assert not [m for m in names(MODULES["only_py"]) if m not in actual]
    leaked = sorted(m for m in names(MODULES["only_node"]) if m in actual)
    assert not leaked, (
        f"exported by cairnq-py but surface.json still calls it Node-only: {leaked}"
    )


def test_declares_every_export_it_publishes():
    known = set(MODULES["shared"]) | set(names(MODULES["only_py"]))
    undeclared = sorted(exported() - known)
    assert not undeclared, (
        "add these to cairnq-protocol/surface.json `modules` — to `shared` (and "
        f"export them from cairnq-node), or to `only_py` with a reason: {undeclared}"
    )


def test_every_declared_export_actually_resolves():
    """`__all__` is a claim about what `from cairnq import X` will produce, and a
    name listed there but never imported raises only at the caller's import."""
    import cairnq

    broken = sorted(n for n in cairnq.__all__ if not hasattr(cairnq, n))
    assert not broken, f"named in __all__ but not importable from cairnq: {broken}"
