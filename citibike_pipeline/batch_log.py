"""The Batch log: one entry per batch, kept as a per-batch history and a rolling 24 h serving copy.

Every key derives from the batch id, so a retry of the same batch overwrites its entry instead of adding one.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import List

import pyarrow as pa

from .batch_log_entry import BatchLogEntry
from .batch_log_row import BatchLogRow
from .counts import Counts as Counts
from .hour_partitions import part_key, partition_path
from .latencies import Latencies as Latencies
from .schemas import BATCH_LOG as BATCH_LOG_SCHEMA
from .schemas import to_table
from .storage import Storage
from .storage_ops import put_parquet, read_parquet

SERVING_BATCH_LOG = "gold/serving/batch_log_recent.parquet"
BATCH_LOG = "ops/batch_log"
RECENT = timedelta(hours=24)

class BatchLog:
    """The Batch log in a lake: record(entry), recent() and day(date)."""

    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    def record(self, entry: BatchLogEntry) -> None:
        """Write the batch's own file, then update the rolling serving copy (deduped by batch id, last 24 h)."""
        put_parquet(self._storage, part_key(BATCH_LOG, entry.as_of, "%s.parquet" % entry.batch_id),
                    _table([entry]))
        kept = [e for e in self.recent(entry.as_of) if e.batch_id != entry.batch_id]
        kept.append(entry)
        recent = sorted(kept, key=lambda e: e.as_of)
        put_parquet(self._storage, SERVING_BATCH_LOG, _table(recent))

    def recent(self, now: datetime) -> List[BatchLogEntry]:
        """The entries with as_of in the 24 h up to `now`, oldest first; empty before the first batch."""
        if not self._storage.exists(SERVING_BATCH_LOG):
            return []
        cutoff = now - RECENT
        return [e for e in _entries(self._storage, SERVING_BATCH_LOG) if e.as_of >= cutoff]

    def day(self, day: date) -> List[BatchLogEntry]:
        """The entries whose as_of falls on this UTC day, across its hour partitions, oldest first."""
        start = datetime.combine(day, time(), tzinfo=timezone.utc)
        out: List[BatchLogEntry] = []
        for h in range(24):
            for key in self._storage.list(partition_path(BATCH_LOG, start + timedelta(hours=h)) + "/"):
                if key.endswith(".parquet"):
                    out.extend(_entries(self._storage, key))
        return sorted(out, key=lambda e: e.as_of)


def _table(entries: List[BatchLogEntry]) -> pa.Table:
    return to_table([BatchLogRow.from_entry(e) for e in entries], BATCH_LOG_SCHEMA)


def _entries(storage: Storage, key: str) -> List[BatchLogEntry]:
    return [BatchLogRow(**row).to_entry() for row in read_parquet(storage, key).to_pylist()]
