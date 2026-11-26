"""One station observation in event time: the input to a station's window."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Obs:
    t: float      # observation time, epoch seconds (fetched_at)
    bikes: int
    docks: int
