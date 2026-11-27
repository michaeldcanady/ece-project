"""Daily data quality report (UTC day). Streams one hour at a time to bound memory."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

from .batch_section import BatchSection
from .config import PipelineConfig
from .daily_quality_report import DailyQualityReport
from .daily_quality_report_thresholds import DailyQualityReportThresholds
from .dlq_section import DLQSection
from .poll_section import PollSection
from .section_status import SectionStatus
from .storage import Storage
from .storage_ops import put_json
from .verdict import Verdict

log = logging.getLogger(__name__)


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def daily_quality(storage: Storage, cfg: PipelineConfig, day: date) -> DailyQualityReport:
    start, end = _day_bounds(day)
    report = DailyQualityReport(
        day_utc=day.isoformat(),
        thresholds=DailyQualityReportThresholds(stale_minutes=cfg.stale_minutes, gap_threshold_s=cfg.gap_threshold_s),
        polls=PollSection.read_day(storage, cfg, start, end),
        station_observations=SectionStatus.read_day(storage, start),
        batches=BatchSection.read_day(storage, start),
        dlq=DLQSection.read_day(storage, start),
    )
    report.verdict = Verdict.of(report)
    log.info("daily quality %s: verdict=%s", day.isoformat(), report.verdict)
    base = "quality/daily/dt=%s" % day.isoformat()
    put_json(storage, base + "/report.json", report.to_json())
    storage.put(base + "/report.md", report.to_markdown().encode("utf-8"))
    return report
