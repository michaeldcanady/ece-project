"""Small metric helpers: percentiles and a station's state. Pure functions."""
from __future__ import annotations

from typing import List, Optional


def pct(sorted_vals: List[float], q: float) -> Optional[float]:
    """Nearest-rank percentile of an already sorted list; None when empty."""
    if not sorted_vals:
        return None
    rank = max(int(-(-q * len(sorted_vals) // 1)) - 1, 0)
    return sorted_vals[min(rank, len(sorted_vals) - 1)]


def station_state(is_installed: bool, is_renting: bool, is_returning: bool, is_stale: bool,
                  bikes: int, docks: int) -> str:
    if not (is_installed and is_renting and is_returning):
        return "OFFLINE"
    if is_stale:
        return "STALE"
    if bikes == 0:
        return "EMPTY"
    if docks == 0:
        return "FULL"
    return "OK"
