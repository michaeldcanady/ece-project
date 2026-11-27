"""The day's Batch log entries, summed. Only built when the day had at least one batch."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from .batch_latency import BatchLatency
from .batch_log import BatchLog
from .percentile import pct
from .storage import Storage


class BatchSection(BaseModel):
    batches: int = 0
    records_read: int = 0
    dlq_rows: int = 0
    duplicates_dropped: int = 0
    replay_rows: int = 0
    latency_s: BatchLatency = Field(default_factory=BatchLatency)

    @staticmethod
    def read_day(storage: Storage, start: datetime) -> Optional[BatchSection]:
        """The section for `start`'s UTC day, or None when no batch ran."""
        rows = BatchLog(storage).day(start.date())
        if not rows:
            return None
        p50 = sorted(r.latencies.p50_s for r in rows if r.latencies.p50_s is not None)
        p95 = sorted(r.latencies.p95_s for r in rows if r.latencies.p95_s is not None)
        return BatchSection(
            batches=len(rows), records_read=sum(r.counts.records_read for r in rows),
            dlq_rows=sum(r.counts.dlq_rows for r in rows),
            duplicates_dropped=sum(r.counts.duplicates_dropped for r in rows),
            replay_rows=sum(r.counts.replay_rows for r in rows),
            latency_s=BatchLatency(
                median_of_batch_p50=pct(p50, 0.5), p95_of_batch_p95=pct(p95, 0.95),
                max=max((r.latencies.max_s for r in rows if r.latencies.max_s is not None), default=None)))
