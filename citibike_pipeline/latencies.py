from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Iterable, Optional

from .metrics import pct

SERVING_BATCH_LOG = "gold/serving/batch_log_recent.parquet"
BATCH_LOG = "ops/batch_log"
RECENT = timedelta(hours=24)

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
