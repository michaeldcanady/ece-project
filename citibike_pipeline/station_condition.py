"""A station's operating state, from its flags and counts."""
from __future__ import annotations

from enum import Enum


class StationCondition(Enum):
    OFFLINE = "OFFLINE"
    STALE = "STALE"
    EMPTY = "EMPTY"
    FULL = "FULL"
    OK = "OK"


def station_condition(is_installed: bool, is_renting: bool, is_returning: bool, is_stale: bool,
                  bikes: int, docks: int) -> StationCondition:
    if not (is_installed and is_renting and is_returning):
        return StationCondition.OFFLINE
    if is_stale:
        return StationCondition.STALE
    if bikes == 0:
        return StationCondition.EMPTY
    if docks == 0:
        return StationCondition.FULL
    return StationCondition.OK
