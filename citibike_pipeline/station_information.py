"""Station information: the payload of the station-information event type, parsed once where it enters the pipeline.

`StationInformation.parse` returns the record, or a `Rejected` with reason codes. The codes are stable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Union

from citibike_contract import JsonObject

from .observations import Rejected, is_int


def _number(value: object) -> Optional[float]:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _optional_text(value: object) -> Optional[str]:
    return value if isinstance(value, str) else None


@dataclass(frozen=True)
class StationInformation:
    """One station's fixed details as the feed reported them."""
    station_id: str
    name: Optional[str]
    short_name: Optional[str]
    lat: float
    lon: float
    capacity: Optional[int]
    region_id: Optional[str]

    @classmethod
    def parse(cls, payload: JsonObject) -> Union[StationInformation, Rejected]:
        """The station information, or why the payload is not one."""
        errors: List[str] = []
        station_id = payload.get("station_id")
        if not isinstance(station_id, str) or not station_id:
            errors.append("bad_station_id")
        lat, lon = _number(payload.get("lat")), _number(payload.get("lon"))
        if lat is None or lon is None:
            errors.append("bad_coordinates")
        capacity = payload.get("capacity")
        if capacity is not None and not is_int(capacity):
            errors.append("bad_capacity")
        if errors or station_id is None or lat is None or lon is None:
            return Rejected(tuple(errors))
        region_id = payload.get("region_id")
        return cls(station_id=str(station_id), name=_optional_text(payload.get("name")),
                   short_name=_optional_text(payload.get("short_name")), lat=lat, lon=lon,
                   capacity=capacity if isinstance(capacity, int) else None,
                   region_id=None if region_id is None else str(region_id))
