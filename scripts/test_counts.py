"""The size of a test suite: how many tests and how many assertions."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TestCounts:
    """Tests (`test_*` functions, module level or in a class) and assertions (`assert` statements plus
    `with pytest.raises(...)` blocks)."""

    __test__ = False  # not a pytest class, despite the name

    tests: int
    assertions: int
