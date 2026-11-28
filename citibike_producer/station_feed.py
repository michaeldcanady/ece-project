"""A GBFS station feed (station_status or station_information) read from its JSON body."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from citibike_contract import JsonObject, JsonValue

from .jsonvalues import mapping_or_empty, optional_int, optional_str


@dataclass(frozen=True)
class StationFeed:
    last_updated: Optional[int]
    ttl: Optional[int]
    version: Optional[str]
    stations: Sequence[JsonValue]      # one item per station; an item that is not an object is not sent

    @staticmethod
    def parse(body: JsonObject) -> Optional[StationFeed]:
        """The feed, or None when `data.stations` is missing or not a list."""
        stations = mapping_or_empty(body.get("data")).get("stations")
        if not isinstance(stations, list):
            return None
        return StationFeed(last_updated=optional_int(body.get("last_updated")), ttl=optional_int(body.get("ttl")),
                           version=optional_str(body.get("version")), stations=stations)
