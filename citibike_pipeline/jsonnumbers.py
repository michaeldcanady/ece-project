"""Checks for the numbers a payload may carry (a bool is not a number here)."""
from __future__ import annotations

from typing import Union

from typing_extensions import TypeGuard


def is_int(x: object) -> TypeGuard[int]:
    return isinstance(x, int) and not isinstance(x, bool)


def is_num(x: object) -> TypeGuard[Union[int, float]]:
    return isinstance(x, (int, float)) and not isinstance(x, bool)
