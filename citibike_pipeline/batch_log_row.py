"""One Batch log entry as a row of the Batch log's flat Parquet columns."""
from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from typing import Literal, Optional

from .batch_log_entry import BatchLogEntry
from .counts import Counts
from .latencies import Latencies


@dataclass(frozen=True)
class BatchLogRow:
    """The columns of `schemas.BATCH_LOG`, in its order."""
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

    @classmethod
    def from_entry(cls, e: BatchLogEntry) -> BatchLogRow:
        return cls(batch_id=e.batch_id, run_id=e.run_id, as_of=e.as_of, planned_at=e.planned_at,
                   published_at=e.published_at, finished_at=e.finished_at, **e.counts.to_json(),
                   latency_p50_s=e.latencies.p50_s, latency_p95_s=e.latencies.p95_s,
                   latency_max_s=e.latencies.max_s, status=e.status)

    def to_entry(self) -> BatchLogEntry:
        counts = Counts(**{f.name: getattr(self, f.name) for f in fields(Counts)})
        return BatchLogEntry(
            batch_id=self.batch_id, run_id=self.run_id, as_of=self.as_of, planned_at=self.planned_at,
            published_at=self.published_at, finished_at=self.finished_at, counts=counts,
            latencies=Latencies(p50_s=self.latency_p50_s, p95_s=self.latency_p95_s, max_s=self.latency_max_s),
            status=self.status)
