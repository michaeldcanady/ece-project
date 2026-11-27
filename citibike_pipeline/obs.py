"""One observation of a station's counts at a time."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Obs:
    t: float      # observation time, epoch seconds (fetched_at)
    bikes: int
    docks: int
