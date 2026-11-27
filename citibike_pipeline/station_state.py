"""A station's state at one as-of time. Pure function."""
from __future__ import annotations

from typing import Literal

StateName = Literal["OFFLINE", "STALE", "EMPTY", "FULL", "OK"]


def station_state(is_installed: bool, is_renting: bool, is_returning: bool, is_stale: bool,
                  bikes: int, docks: int) -> StateName:
    if not (is_installed and is_renting and is_returning):
        return "OFFLINE"
    if is_stale:
        return "STALE"
    if bikes == 0:
        return "EMPTY"
    if docks == 0:
        return "FULL"
    return "OK"
