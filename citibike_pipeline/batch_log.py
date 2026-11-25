"""The Batch log: one entry per batch, kept as a per-batch history and a rolling 24 h serving copy.

Every key derives from the batch id, so a retry of the same batch overwrites its entry instead of adding one.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING, Iterable, List, Literal, Mapping, Optional

import pyarrow as pa

from . import lake
from .metrics import pct
from .schemas import BATCH_LOG as BATCH_LOG_SCHEMA
from .schemas import to_table
from .storage import Storage, put_parquet, read_parquet

if TYPE_CHECKING:
    from .batch import Plan, Published, Stats

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
    def from_json(cls, obj: Mapping[str, object]) -> Counts:
        """Rebuild from flat keys; a missing key raises KeyError."""
        return cls(**{f.name: int(obj[f.name]) for f in fields(cls)})


@dataclass(frozen=True)
class Latencies:
    """Seconds from observation to serving, summarised over a batch's status rows; None when it had none."""
    p50_s: Optional[float]
    p95_s: Optional[float]
    max_s: Optional[float]

    @classmethod
    def from_seconds(cls, seconds: Iterable[float]) -> Latencies:
        lat = sorted(seconds)
        return cls(p50_s=pct(lat, 0.50), p95_s=pct(lat, 0.95), max_s=lat[-1] if lat else None)


@dataclass(frozen=True)
class BatchLogEntry:
    """A batch's counts and latencies, as written when it finished."""
    batch_id: str
    run_id: str
    as_of: datetime
    planned_at: datetime
    published_at: Optional[datetime]
    finished_at: datetime
    counts: Counts
    latencies: Latencies
    status: Literal["success"]

    @classmethod
    def from_batch(cls, plan: Plan, stats: Stats, published: Published, latencies: Latencies,
                   finished_at: datetime) -> BatchLogEntry:
        """The entry for a batch that went through every step."""
        return cls(batch_id=plan.batch_id, run_id=plan.run_id, as_of=stats.as_of, planned_at=plan.planned_at,
                   published_at=published.published_at, finished_at=finished_at, counts=stats.counts,
                   latencies=latencies, status="success")


class BatchLog:
    """The Batch log in a lake: record(entry), recent() and day(date)."""

    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    def record(self, entry: BatchLogEntry) -> None:
        """Write the batch's own file, then update the rolling serving copy (deduped by batch id, last 24 h)."""
        put_parquet(self._storage, lake.part_key(BATCH_LOG, entry.as_of, "%s.parquet" % entry.batch_id),
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
            for key in self._storage.list(lake.partition_path(BATCH_LOG, start + timedelta(hours=h)) + "/"):
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
