from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import timedelta
from typing import Mapping

from citibike_contract.json_value import JsonObject

from .jsonfields import as_int

SERVING_BATCH_LOG = "gold/serving/batch_log_recent.parquet"
BATCH_LOG = "ops/batch_log"
RECENT = timedelta(hours=24)


@dataclass(frozen=True)
class Counts:
    """What a batch read, wrote and flagged. Flat on the wire: the Stats JSON and the Batch log columns."""
    records_read: int
    raw_rows: int
    status_rows: int
    weather_rows: int
    dlq_rows: int
    duplicates_dropped: int
    repeats: int
    out_of_order: int
    stale: int
    unknown_station: int
    replay_rows: int

    def to_json(self) -> Mapping[str, int]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_json(cls, obj: JsonObject) -> Counts:
        """Rebuild from flat keys; a missing key raises KeyError."""
        return cls(**{f.name: as_int(obj[f.name]) for f in fields(cls)})
