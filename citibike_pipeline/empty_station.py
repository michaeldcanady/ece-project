"""A station that was empty in some of the day's observations."""
from __future__ import annotations

from pydantic import BaseModel


class EmptyStation(BaseModel):
    station_id: str
    empty_share: float
    empty_observations: int
