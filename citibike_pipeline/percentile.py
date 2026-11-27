"""Nearest-rank percentile of a sorted list. Pure function."""
from __future__ import annotations

from typing import List, Optional


def pct(sorted_vals: List[float], q: float) -> Optional[float]:
    """Nearest-rank percentile of an already sorted list; None when empty."""
    if not sorted_vals:
        return None
    rank = max(int(-(-q * len(sorted_vals) // 1)) - 1, 0)
    return sorted_vals[min(rank, len(sorted_vals) - 1)]
