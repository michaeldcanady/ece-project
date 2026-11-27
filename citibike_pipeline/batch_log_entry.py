from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Optional

from .counts import Counts
from .latencies import Latencies

if TYPE_CHECKING:
    from .batch import Plan, Published, Stats

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
