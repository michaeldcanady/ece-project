"""The Batch log: one entry per batch, kept as a per-batch history and a rolling 24 h serving copy.

Every key derives from the batch id, so a retry of the same batch overwrites its entry instead of adding one.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import List, Mapping

import pyarrow as pa

from .batch_log_entry import BatchLogEntry
from .counts import Counts
from .hour_partitions import part_key, partition_path
from .latencies import Latencies
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
    return to_table([_flat(e) for e in entries], BATCH_LOG_SCHEMA)


def _flat(e: BatchLogEntry) -> Mapping[str, object]:
    """The entry as one row of the Batch log's flat columns."""
    return {"batch_id": e.batch_id, "run_id": e.run_id, "as_of": e.as_of, "planned_at": e.planned_at,
            "published_at": e.published_at, "finished_at": e.finished_at, **e.counts.to_json(),
            "latency_p50_s": e.latencies.p50_s, "latency_p95_s": e.latencies.p95_s,
            "latency_max_s": e.latencies.max_s, "status": e.status}


def _entries(storage: Storage, key: str) -> List[BatchLogEntry]:
    return [_entry(row) for row in read_parquet(storage, key).to_pylist()]


def _entry(row: Mapping[str, object]) -> BatchLogEntry:
    return BatchLogEntry(
        batch_id=row["batch_id"], run_id=row["run_id"], as_of=row["as_of"], planned_at=row["planned_at"],
        published_at=row["published_at"], finished_at=row["finished_at"], counts=Counts.from_json(row),
        latencies=Latencies(p50_s=row["latency_p50_s"], p95_s=row["latency_p95_s"], max_s=row["latency_max_s"]),
        status=row["status"])
