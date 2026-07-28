"""One version, one place. `scripts/bump-version.mjs` keeps package.json and
pyproject.toml in step, but `__version__` was a third copy it never touched — so a
release could ship a package whose own `__version__` disagreed with it."""

from __future__ import annotations

import tomllib
from pathlib import Path

import cairnq

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def test_dunder_version_matches_the_packaging_metadata():
    declared = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]["version"]
    assert cairnq.__version__ == declared
