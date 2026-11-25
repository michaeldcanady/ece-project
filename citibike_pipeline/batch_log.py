"""The Batch log: one entry per batch, kept as a per-batch history and a rolling 24 h serving copy.

Every key derives from the batch id, so a retry of the same batch overwrites its entry instead of adding one.
"""
from __future__ import annotations

from dataclasses import astuple, dataclass, fields
from datetime import date, datetime, time, timedelta, timezone
from typing import List, Literal, Optional

import pyarrow as pa

from . import lake
from .schemas import BATCH_LOG as BATCH_LOG_SCHEMA
from .schemas import to_table
from .storage import Storage, put_parquet, read_parquet

SERVING_BATCH_LOG = "gold/serving/batch_log_recent.parquet"
BATCH_LOG = "ops/batch_log"
RECENT = timedelta(hours=24)


@dataclass(frozen=True)
class BatchLogEntry:
    """A batch's counts and latencies, as written when it finished."""
    batch_id: str
    run_id: str
    as_of: datetime
    planned_at: datetime
    published_at: Optional[datetime]
    finished_at: datetime
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
    latency_p50_s: Optional[float]
    latency_p95_s: Optional[float]
    latency_max_s: Optional[float]
    status: Literal["success"]


class BatchLog:
    """The Batch log in a lake: record(entry), recent() and day(date)."""

    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    def record(self, entry: BatchLogEntry) -> None:
        """Write the batch's own file, then update the rolling serving copy (deduped by batch id, last 24 h)."""
        put_parquet(self._storage, lake.part_key(BATCH_LOG, entry.as_of, "%s.parquet" % entry.batch_id),
                    _table([entry]))
        kept = [e for e in self.recent() if e.batch_id != entry.batch_id]
        kept.append(entry)
        cutoff = entry.as_of - RECENT
        recent = sorted((e for e in kept if e.as_of >= cutoff), key=lambda e: e.as_of)
        put_parquet(self._storage, SERVING_BATCH_LOG, _table(recent))

    def recent(self) -> List[BatchLogEntry]:
        """The entries of the last 24 h, oldest first; empty before the first batch."""
        if not self._storage.exists(SERVING_BATCH_LOG):
            return []
        return _entries(self._storage, SERVING_BATCH_LOG)

    def day(self, day: date) -> List[BatchLogEntry]:
        """The entries whose as_of falls on this UTC day, across its hour partitions, oldest first."""
        start = datetime.combine(day, time(), tzinfo=timezone.utc)
        out: List[BatchLogEntry] = []
        for h in range(24):
            for key in self._storage.list(lake.partition_path(BATCH_LOG, start + timedelta(hours=h)) + "/"):
                if key.endswith(".parquet"):
                    out.extend(_entries(self._storage, key))
        return sorted(out, key=lambda e: e.as_of)


def _table(entries: List[BatchLogEntry]) -> pa.Table:
    names = [f.name for f in fields(BatchLogEntry)]
    return to_table([dict(zip(names, astuple(e))) for e in entries], BATCH_LOG_SCHEMA)


def _entries(storage: Storage, key: str) -> List[BatchLogEntry]:
    return [BatchLogEntry(**row) for row in read_parquet(storage, key).to_pylist()]
