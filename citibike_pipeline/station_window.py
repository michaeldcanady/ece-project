"""One station's window in a Snapshot, as written to the gold windows table."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .timeutil import from_epoch
from .window_metrics import WindowMetrics


@dataclass(frozen=True)
class StationWindow:
    station_id: str
    as_of: datetime
    window_start: datetime
    n_obs: int
    coverage_min: float
    bikes_first: int
    bikes_last: int
    docks_first: int
    docks_last: int
    net_change: int
    rate_per_min: Optional[float]
    docks_rate_per_min: Optional[float]
    minutes_to_empty: Optional[float]
    minutes_to_full: Optional[float]
    at_risk_empty: bool
    at_risk_full: bool
    batch_id: str

    @classmethod
    def from_metrics(cls, station_id: str, as_of: datetime, batch_id: str, m: WindowMetrics) -> StationWindow:
        return cls(station_id=station_id, as_of=as_of, window_start=from_epoch(m.window_start),
                   n_obs=m.n_obs, coverage_min=m.coverage_min,
                   bikes_first=m.bikes_first, bikes_last=m.bikes_last,
                   docks_first=m.docks_first, docks_last=m.docks_last,
                   net_change=m.net_change, rate_per_min=m.rate_per_min,
                   docks_rate_per_min=m.docks_rate_per_min,
                   minutes_to_empty=m.minutes_to_empty, minutes_to_full=m.minutes_to_full,
                   at_risk_empty=m.at_risk_empty, at_risk_full=m.at_risk_full, batch_id=batch_id)
