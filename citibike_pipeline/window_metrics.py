"""A station's 15-minute window by rate extrapolation (no ML), and when it counts as at risk. Pure, event time."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Sequence

from .config import PipelineConfig
from .obs import Obs

AtRisk = Literal["EMPTY", "FULL"]


@dataclass(frozen=True)
class WindowMetrics:
    """The observations of one station over `(as_of - window_minutes, as_of]` and every rule derived from them."""
    window_start: float           # epoch seconds
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

    @classmethod
    def over(cls, obs: Sequence[Obs], as_of: float, cfg: PipelineConfig) -> Optional[WindowMetrics]:
        """The window ending at `as_of`; None when no observation falls inside it."""
        start = as_of - cfg.window_minutes * 60
        w = sorted((o for o in obs if start < o.t <= as_of), key=lambda o: o.t)
        if not w:
            return None
        first, last = w[0], w[-1]
        coverage = (last.t - first.t) / 60.0
        rate: Optional[float] = None
        docks_rate: Optional[float] = None
        mte: Optional[float] = None
        mtf: Optional[float] = None
        at_risk_empty = at_risk_full = False
        if len(w) < 2 or coverage < cfg.min_coverage_minutes:
            # too little history to extrapolate; an already empty/full station still reports 0
            if last.bikes == 0:
                mte = 0.0
            if last.docks == 0:
                mtf = 0.0
        else:
            rate = (last.bikes - first.bikes) / coverage
            docks_rate = (last.docks - first.docks) / coverage
            if last.bikes == 0:
                mte = 0.0
            elif rate < 0:
                mte = last.bikes / -rate
            if last.docks == 0:
                mtf = 0.0
            elif docks_rate < 0:
                mtf = last.docks / -docks_rate

            noise = cfg.noise_floor_bikes
            at_risk_empty = (last.bikes > 0 and mte is not None and mte <= cfg.risk_horizon_minutes
                             and (first.bikes - last.bikes) >= noise)
            at_risk_full = (last.docks > 0 and mtf is not None and mtf <= cfg.risk_horizon_minutes
                            and (first.docks - last.docks) >= noise)
        return cls(window_start=start, n_obs=len(w), coverage_min=coverage,
                   bikes_first=first.bikes, bikes_last=last.bikes,
                   docks_first=first.docks, docks_last=last.docks,
                   net_change=last.bikes - first.bikes,
                   rate_per_min=rate, docks_rate_per_min=docks_rate,
                   minutes_to_empty=mte, minutes_to_full=mtf,
                   at_risk_empty=at_risk_empty, at_risk_full=at_risk_full)

    @property
    def at_risk(self) -> Optional[AtRisk]:
        """Which way the station is at risk; empty is named before full."""
        if self.at_risk_empty:
            return "EMPTY"
        if self.at_risk_full:
            return "FULL"
        return None
