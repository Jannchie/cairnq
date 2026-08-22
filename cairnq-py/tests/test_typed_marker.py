"""The PEP 561 marker, without which this package's annotations are dead weight.

mypy skips an installed package that does not carry `py.typed` — every public
type here (`Task`, `TaskDef`, `Worker`, `TaskContext`) then reaches the caller as
`Any`, silently. It is one empty file, so nothing fails when it goes missing;
this is what notices."""

from __future__ import annotations

from pathlib import Path

import cairnq

PACKAGE = Path(cairnq.__file__).resolve().parent


def test_the_package_ships_a_py_typed_marker():
    # Resolved through the imported package rather than the source tree, so this
    # also holds for an installed wheel — which is the case that matters.
    assert (PACKAGE / "py.typed").is_file()
