"""The overall health of a day's data, from its quality report."""
from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .daily_quality_report import DailyQualityReport


class Verdict(Enum):
    OK = "ok"
    WARN = "warn"
    FAIL = "fail"

    @staticmethod
    def of(report: DailyQualityReport) -> Verdict:
        status = report.station_observations
        poll = report.polls.get("station_status")
        if not status.observations:
            return Verdict.FAIL
        completeness = (poll.completeness if poll else None) or 0
        records_read = report.batches.records_read if report.batches else 0
        dlq_rate = report.dlq.total / float(max(records_read, 1))
        if completeness < 0.5:
            return Verdict.FAIL
        if completeness < 0.95 or (status.stale_share or 0) > 0.05 or dlq_rate > 0.01:
            return Verdict.WARN
        return Verdict.OK
