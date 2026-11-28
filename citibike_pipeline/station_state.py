"""A station's state at one as-of time: the StationState record and the rule that sets its `state`."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from .window_metrics import AtRisk

StateName = Literal["OFFLINE", "STALE", "EMPTY", "FULL", "OK"]


@dataclass(frozen=True)
class StationState:
    station_id: str
    station_name: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    capacity: Optional[int]
    state: StateName
    num_bikes_available: int
    num_ebikes_available: int
    num_docks_available: int
    last_reported: int
    age_s: int
    observed_at: datetime
    net_change_15m: Optional[int]
    rate_per_min: Optional[float]
    minutes_to_empty: Optional[float]
    minutes_to_full: Optional[float]
    at_risk: Optional[AtRisk]
    temperature_c: Optional[float]
    precipitation_mm: Optional[float]
    as_of: datetime
    batch_id: str


def station_state(is_installed: bool, is_renting: bool, is_returning: bool, is_stale: bool,  # noqa: PLR0913
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
