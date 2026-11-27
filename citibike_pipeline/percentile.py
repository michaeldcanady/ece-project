"""Nearest-rank percentile of a sorted list. Pure function."""
from __future__ import annotations

from typing import Optional, Sequence, TypeVar

_N = TypeVar("_N", int, float)


def pct(sorted_vals: Sequence[_N], q: float) -> Optional[_N]:
    """Nearest-rank percentile of an already sorted list; None when empty. Keeps the type of the values."""
    if not sorted_vals:
        return None
    rank = max(int(-(-q * len(sorted_vals) // 1)) - 1, 0)
    return sorted_vals[min(rank, len(sorted_vals) - 1)]
