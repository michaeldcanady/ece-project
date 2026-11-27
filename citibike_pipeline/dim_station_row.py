"""One row of the station dimension table: a Station information and the Kafka message it came from."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from citibike_contract import Envelope

from .kafka_io import Record
from .station_information import StationInformation


@dataclass(frozen=True)
class DimStationRow:
    station_id: str
    name: Optional[str]
    short_name: Optional[str]
    lat: float
    lon: float
    capacity: Optional[int]
    region_id: Optional[str]
    feed_last_updated: Optional[int]
    fetched_at: datetime
    kafka_partition: int
    kafka_offset: int
    snapshot_at: datetime

    @classmethod
    def from_delivered(cls, info: StationInformation, envelope: Envelope, record: Record,
                       snapshot_at: datetime) -> DimStationRow:
        return cls(station_id=info.station_id, name=info.name, short_name=info.short_name, lat=info.lat,
                   lon=info.lon, capacity=info.capacity, region_id=info.region_id,
                   feed_last_updated=envelope.feed_last_updated, fetched_at=envelope.fetched_at,
                   kafka_partition=record.partition, kafka_offset=record.offset, snapshot_at=snapshot_at)
