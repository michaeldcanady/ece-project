"""The curated rows Enrichment writes: one class per lake table, its fields in the table schema's order.

`schemas.py` stays the source of truth for the Parquet types; tests/test_rows.py checks these classes match it.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List, Mapping, Optional, Sequence

from .delivered import Delivered
from .weather_observation import WeatherObservation


@dataclass(frozen=True)
class CuratedStatusRow:
    """One station-status observation with its quality flags, station dimension fields and weather."""
    station_id: str
    fetched_at: datetime
    last_reported: int
    feed_last_updated: Optional[int]
    num_bikes_available: int
    num_ebikes_available: Optional[int]
    num_docks_available: int
    num_bikes_disabled: Optional[int]
    num_docks_disabled: Optional[int]
    is_installed: bool
    is_renting: bool
    is_returning: bool
    age_s: int
    is_stale: bool
    is_repeat: bool
    is_out_of_order: bool
    is_unknown_station: bool
    station_name: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    capacity: Optional[int]
    temperature_c: Optional[float]
    precipitation_mm: Optional[float]
    weather_code: Optional[int]
    wind_speed_kmh: Optional[float]
    poll_id: str
    replay: bool
    replayed_at: Optional[datetime]
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp: Optional[datetime]
    batch_id: str
    processed_at: datetime


@dataclass(frozen=True)
class CuratedWeatherRow:
    """One weather observation with the Kafka identity it came from."""
    location_id: Optional[str]
    obs_time: datetime
    fetched_at: datetime
    temperature_c: Optional[float]
    relative_humidity: Optional[float]
    precipitation_mm: Optional[float]
    rain_mm: Optional[float]
    snowfall_cm: Optional[float]
    weather_code: Optional[int]
    cloud_cover: Optional[float]
    wind_speed_kmh: Optional[float]
    poll_id: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    batch_id: str
    processed_at: datetime

    @classmethod
    def from_delivered(cls, d: Delivered[WeatherObservation], batch_id: str,
                       processed_at: datetime) -> CuratedWeatherRow:
        w, env, rec = d.value, d.envelope, d.record
        return cls(location_id=w.location_id, obs_time=w.obs_time, fetched_at=env.fetched_at,
                   temperature_c=w.temperature_c, relative_humidity=w.relative_humidity,
                   precipitation_mm=w.precipitation_mm, rain_mm=w.rain_mm, snowfall_cm=w.snowfall_cm,
                   weather_code=w.weather_code, cloud_cover=w.cloud_cover, wind_speed_kmh=w.wind_speed_kmh,
                   poll_id=env.poll_id, kafka_topic=rec.topic, kafka_partition=rec.partition,
                   kafka_offset=rec.offset, batch_id=batch_id, processed_at=processed_at)


def as_mappings(rows: Sequence[CuratedStatusRow | CuratedWeatherRow]) -> List[Mapping[str, object]]:
    """The rows as the column-name mappings `lake.write` takes."""
    return [asdict(r) for r in rows]
