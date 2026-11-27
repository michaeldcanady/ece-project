"""Station status: the payload of the station-status event type, parsed once where it enters the pipeline.

`StationStatus.parse` returns the record, or a `Rejected` with the reason codes the dead-letter rows carry. Past the
parse, code reads attributes. The reason codes are what the DLQ stores, so they are stable."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Union

from citibike_contract import JsonObject

from .jsonnumbers import is_int
from .rejected import Rejected

BOOL_LIKE = (0, 1, True, False)


def _optional_count(payload: JsonObject, field: str, errors: List[str]) -> Optional[int]:
    """An optional non-negative count: absent is None, present and malformed adds `bad_<field>`."""
    if field not in payload:
        return None
    value = payload[field]
    if not is_int(value) or value < 0:
        errors.append("bad_" + field)
        return None
    return value


@dataclass(frozen=True)
class StationStatus:
    """One station's bike and dock counts as the feed reported them at `last_reported` (epoch seconds)."""
    station_id: str
    last_reported: int
    num_bikes_available: int
    num_docks_available: int
    num_ebikes_available: Optional[int]
    num_bikes_disabled: Optional[int]
    num_docks_disabled: Optional[int]
    is_installed: bool
    is_renting: bool
    is_returning: bool

    @classmethod
    def parse(cls, payload: JsonObject, fetched_s: float, future_skew_s: int) -> Union[StationStatus, Rejected]:
        """The station status, or why the payload is not one. `last_reported` may run `future_skew_s` ahead
        of `fetched_s` (the fetch time, epoch seconds) before it counts as in the future."""
        errors: List[str] = []
        station_id = payload.get("station_id")
        if not isinstance(station_id, str) or not station_id:
            errors.append("bad_station_id")
        counts = {}
        for field in ("num_bikes_available", "num_docks_available"):
            value = payload.get(field)
            if not is_int(value) or value < 0:
                errors.append("bad_" + field)
            counts[field] = value
        ebikes = _optional_count(payload, "num_ebikes_available", errors)
        bikes_disabled = _optional_count(payload, "num_bikes_disabled", errors)
        docks_disabled = _optional_count(payload, "num_docks_disabled", errors)
        flags = {}
        for field in ("is_installed", "is_renting", "is_returning"):
            if field in payload and payload[field] not in BOOL_LIKE:
                errors.append("bad_" + field)
            flags[field] = bool(payload.get(field, 1))
        last_reported = payload.get("last_reported")
        if last_reported is None:
            errors.append("missing_last_reported")
        elif not is_int(last_reported) or last_reported < 0:
            errors.append("bad_last_reported")
        elif last_reported > fetched_s + future_skew_s:
            errors.append("last_reported_in_future")
        if errors:
            return Rejected(tuple(errors))
        return cls(station_id=str(station_id), last_reported=int(last_reported),
                   num_bikes_available=int(counts["num_bikes_available"]),
                   num_docks_available=int(counts["num_docks_available"]),
                   num_ebikes_available=ebikes, num_bikes_disabled=bikes_disabled,
                   num_docks_disabled=docks_disabled, **flags)
