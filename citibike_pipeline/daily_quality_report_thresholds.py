"""The thresholds a Quality report was judged against."""
from __future__ import annotations

from pydantic import BaseModel


class DailyQualityReportThresholds(BaseModel):
    stale_minutes: float
    gap_threshold_s: int
