"""Checks for the numbers a payload may carry (a bool is not a number here)."""
from __future__ import annotations


def is_int(x: object) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def is_num(x: object) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)
