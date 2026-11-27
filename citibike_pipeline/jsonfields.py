"""Reading values out of parsed JSON: each function narrows an `object` or raises ValueError."""
from __future__ import annotations

from typing import Mapping

from citibike_contract import JsonObject


def as_object(value: object) -> JsonObject:
    if not isinstance(value, Mapping):
        raise ValueError("expected a JSON object, got %r" % (value,))
    return value


def as_int(value: object) -> int:
    """An integer, or a float or numeric string that reads as one (bool is not a number here)."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("expected an integer, got %r" % (value,))
    return int(value)
