"""The metrics of one station over its window."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class WindowMetrics:
    window_start: float
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
