"""The Station information table: the latest row per station, with its keys, write and read in one place."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Optional, Sequence

from .dim_station_row import DimStationRow
from .schemas import DIM_STATION, to_table
from .station_information import StationInformation
from .storage import Storage
from .storage_ops import put_parquet, read_parquet

_CURRENT = "curated/dim_station/current.parquet"


def _snapshot_key(at: datetime) -> str:
    return "curated/dim_station/snapshot_dt=%s/hr=%02d/dim_station.parquet" % (at.strftime("%Y-%m-%d"), at.hour)


@dataclass(frozen=True)
class StationInformationTable:
    def replace(self, storage: Storage, rows: Sequence[DimStationRow], snapshot_at: datetime) -> None:
        """Write the snapshot copy and the current table. Raises ValueError on no rows, leaving current as is."""
        if not rows:
            raise ValueError("refusing to replace the station information table with an empty one")
        table = to_table([r for r in sorted(rows, key=lambda r: r.station_id)], DIM_STATION)
        put_parquet(storage, _snapshot_key(snapshot_at), table)
        put_parquet(storage, _CURRENT, table)

    def current(self, storage: Storage) -> Optional[Mapping[str, StationInformation]]:
        """The stations by id, or None before the first build."""
        if not storage.exists(_CURRENT):
            return None
        return {s.station_id: s for s in StationInformation.from_table(read_parquet(storage, _CURRENT))}
