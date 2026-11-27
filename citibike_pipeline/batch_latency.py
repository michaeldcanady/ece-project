"""How long a day's batches took, summarised."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class BatchLatency(BaseModel):
    median_of_batch_p50: Optional[float] = None
    p95_of_batch_p95: Optional[float] = None
    max: Optional[float] = None
