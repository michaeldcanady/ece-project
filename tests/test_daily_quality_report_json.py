from typing import Mapping, Optional

from citibike_pipeline.batch_section import BatchSection
from citibike_pipeline.daily_quality_report import DailyQualityReport
from citibike_pipeline.daily_quality_report_thresholds import DailyQualityReportThresholds
from citibike_pipeline.dlq_section import DLQSection
from citibike_pipeline.poll_section import PollSection
from citibike_pipeline.section_status import SectionStatus

_GAP_FIELDS = {"expected_polls", "completeness", "gaps_over_threshold", "gap_seconds_total", "largest_gaps"}


def _report(polls: Mapping[str, PollSection], batches: Optional[BatchSection] = None) -> DailyQualityReport:
    return DailyQualityReport(
        day_utc="2025-01-01", thresholds=DailyQualityReportThresholds(stale_minutes=10, gap_threshold_s=60),
        polls=polls,
        station_observations=SectionStatus(
            observations=0, distinct_stations=0, stale_share=None, repeat_share=None, out_of_order=0,
            unknown_station_rows=0, age_cdf={}, top_empty_stations=[], empty_share_by_hour_utc={}),
        batches=batches, dlq=DLQSection(total=0, by_reason={}))


def _poll(measured: bool) -> PollSection:
    gaps = dict(expected_polls=4, completeness=0.75, gaps_over_threshold=0, gap_seconds_total=0,
                largest_gaps=[]) if measured else {}
    return PollSection(polls_logged=3, outcomes={"ok": 3}, http_latency_ms_p50=10, http_latency_ms_p95=20, **gaps)


def test_measured_job_keeps_its_gap_fields_and_unmeasured_job_drops_them() -> None:
    out = _report({"station_status": _poll(True), "weather": _poll(False)}).to_json()
    polls = out["polls"]
    assert isinstance(polls, dict)
    assert _GAP_FIELDS <= set(polls["station_status"])
    assert not _GAP_FIELDS & set(polls["weather"])
    assert {"polls_logged", "outcomes"} <= set(polls["weather"])


def test_day_without_batches_reports_just_a_zero_count() -> None:
    assert _report({}).to_json()["batches"] == {"batches": 0}
    with_batches = _report({}, BatchSection(batches=2)).to_json()["batches"]
    assert isinstance(with_batches, dict) and with_batches["batches"] == 2
