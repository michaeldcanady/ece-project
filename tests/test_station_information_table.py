from datetime import datetime, timezone

import pytest

from citibike_pipeline.dim_station_row import DimStationRow
from citibike_pipeline.local_storage import LocalStorage
from citibike_pipeline.station_information_table import StationInformationTable

AT = datetime(2025, 1, 2, 3, tzinfo=timezone.utc)


def row(station_id: str, capacity: int) -> DimStationRow:
    return DimStationRow(station_id=station_id, name="n", short_name=None, lat=40.0, lon=-73.0, capacity=capacity,
                         region_id="71", feed_last_updated=1, fetched_at=AT, kafka_partition=0, kafka_offset=1,
                         snapshot_at=AT)


def test_current_is_none_before_the_first_replace(tmp_path):
    assert StationInformationTable().current(LocalStorage(str(tmp_path))) is None


def test_replace_then_current_round_trips_and_writes_the_snapshot(tmp_path):
    storage = LocalStorage(str(tmp_path))
    StationInformationTable().replace(storage, [row("a", 10), row("b", 20)], AT)
    current = StationInformationTable().current(storage)
    assert current is not None and {k: v.capacity for k, v in current.items()} == {"a": 10, "b": 20}
    assert storage.exists("curated/dim_station/snapshot_dt=2025-01-02/hr=03/dim_station.parquet")


def test_second_replace_overwrites_current(tmp_path):
    storage = LocalStorage(str(tmp_path))
    StationInformationTable().replace(storage, [row("a", 10)], AT)
    StationInformationTable().replace(storage, [row("c", 5)], AT)
    current = StationInformationTable().current(storage)
    assert current is not None and list(current) == ["c"]


def test_empty_replace_raises_and_keeps_current(tmp_path):
    storage = LocalStorage(str(tmp_path))
    StationInformationTable().replace(storage, [row("a", 10)], AT)
    with pytest.raises(ValueError):
        StationInformationTable().replace(storage, [], AT)
    current = StationInformationTable().current(storage)
    assert current is not None and list(current) == ["a"]
