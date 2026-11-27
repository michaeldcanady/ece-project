"""Station status and Weather observation: the payloads of the two event types the Batch turns into curated rows,
parsed once where they enter the pipeline.

`StationStatus.parse` and `WeatherObservation.parse` return the record, or a `Rejected` with the reason codes the
dead-letter rows carry. Past the parse, code reads attributes: nothing re-checks a payload field. The reason
codes are what the DLQ stores, so they are stable.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, List, Mapping, Optional, Tuple, TypeVar, Union

from citibike_contract import Envelope

from .kafka_io import Record
from .timeutil import from_epoch

T = TypeVar("T")

JsonObject = Mapping[str, object]
BOOL_LIKE = (0, 1, True, False)


@dataclass(frozen=True)
class Rejected:
    """A payload that failed its parse, with every reason it failed."""
    reasons: Tuple[str, ...]


@dataclass(frozen=True)
class Delivered(Generic[T]):
    """A parsed observation with the Kafka record and the envelope it arrived in."""
    record: Record
    envelope: Envelope
    value: T


def is_int(x: object) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def is_num(x: object) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


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


@dataclass(frozen=True)
class WeatherObservation:
    """The current weather at a location. Only the time and the temperature are checked by the parse; the
    other measures pass through as the source sent them."""
    location_id: Optional[str]
    obs_time: datetime
    temperature_c: Optional[float]
    relative_humidity: Optional[float]
    precipitation_mm: Optional[float]
    rain_mm: Optional[float]
    snowfall_cm: Optional[float]
    weather_code: Optional[int]
    cloud_cover: Optional[float]
    wind_speed_kmh: Optional[float]

    @classmethod
    def parse(cls, payload: JsonObject) -> Union[WeatherObservation, Rejected]:
        """The weather observation, or why the payload is not one."""
        current = payload.get("current")
        if not isinstance(current, dict):
            return Rejected(("missing_current",))
        errors = []
        if not is_num(current.get("time")):
            errors.append("bad_obs_time")
        temperature = current.get("temperature_2m")
        if temperature is not None and not is_num(temperature):
            errors.append("bad_temperature")
        if errors:
            return Rejected(tuple(errors))
        return cls(
            location_id=payload.get("location_id"),
            obs_time=from_epoch(current["time"]), temperature_c=temperature,
            relative_humidity=current.get("relative_humidity_2m"), precipitation_mm=current.get("precipitation"),
            rain_mm=current.get("rain"), snowfall_cm=current.get("snowfall"),
            weather_code=current.get("weather_code"), cloud_cover=current.get("cloud_cover"),
            wind_speed_kmh=current.get("wind_speed_10m"))
