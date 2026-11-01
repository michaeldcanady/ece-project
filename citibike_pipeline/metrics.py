"""Window metrics by rate extrapolation (no ML). Pure functions over event time."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .config import PipelineConfig


def pct(sorted_vals: List[float], q: float) -> Optional[float]:
    """Nearest-rank percentile of an already sorted list; None when empty."""
    if not sorted_vals:
        return None
    rank = max(int(-(-q * len(sorted_vals) // 1)) - 1, 0)
    return sorted_vals[min(rank, len(sorted_vals) - 1)]


@dataclass
class Obs:
    t: float      # observation time, epoch seconds (fetched_at)
    bikes: int
    docks: int


def window_metrics(obs: List[Obs], as_of: float, cfg: PipelineConfig) -> Optional[Dict[str, Any]]:
    start = as_of - cfg.window_minutes * 60
    w = sorted((o for o in obs if start < o.t <= as_of), key=lambda o: o.t)
    if not w:
        return None
    first, last = w[0], w[-1]
    coverage = (last.t - first.t) / 60.0
    out: Dict[str, Any] = {
        "window_start": start, "n_obs": len(w), "coverage_min": coverage,
        "bikes_first": first.bikes, "bikes_last": last.bikes,
        "docks_first": first.docks, "docks_last": last.docks,
        "net_change": last.bikes - first.bikes,
        "rate_per_min": None, "docks_rate_per_min": None,
        "minutes_to_empty": None, "minutes_to_full": None,
        "at_risk_empty": False, "at_risk_full": False,
    }
    if len(w) < 2 or coverage < cfg.min_coverage_minutes:
        # too little history to extrapolate; an already empty/full station still reports 0
        if last.bikes == 0:
            out["minutes_to_empty"] = 0.0
        if last.docks == 0:
            out["minutes_to_full"] = 0.0
        return out

    bike_rate = (last.bikes - first.bikes) / coverage
    dock_rate = (last.docks - first.docks) / coverage
    out["rate_per_min"], out["docks_rate_per_min"] = bike_rate, dock_rate
    if last.bikes == 0:
        out["minutes_to_empty"] = 0.0
    elif bike_rate < 0:
        out["minutes_to_empty"] = last.bikes / -bike_rate
    if last.docks == 0:
        out["minutes_to_full"] = 0.0
    elif dock_rate < 0:
        out["minutes_to_full"] = last.docks / -dock_rate

    noise = cfg.noise_floor_bikes
    mte, mtf = out["minutes_to_empty"], out["minutes_to_full"]
    out["at_risk_empty"] = (last.bikes > 0 and mte is not None and mte <= cfg.risk_horizon_minutes
                            and (first.bikes - last.bikes) >= noise)
    out["at_risk_full"] = (last.docks > 0 and mtf is not None and mtf <= cfg.risk_horizon_minutes
                           and (first.docks - last.docks) >= noise)
    return out


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
