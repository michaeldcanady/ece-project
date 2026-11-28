"""The Quality report for one UTC day."""
from __future__ import annotations

from typing import Mapping, Optional

from pydantic import BaseModel

from citibike_contract.json_value import JsonObject

from .batch_section import BatchSection
from .daily_quality_report_thresholds import DailyQualityReportThresholds
from .dlq_section import DLQSection
from .poll_section import PollSection
from .section_status import SectionStatus
from .verdict import Verdict


class DailyQualityReport(BaseModel):
    day_utc: str
    thresholds: DailyQualityReportThresholds
    polls: Mapping[str, PollSection]
    station_observations: SectionStatus
    batches: Optional[BatchSection]
    dlq: DLQSection
    verdict: Optional[Verdict] = None

    def to_json(self) -> JsonObject:
        """The report.json object: a day with no batches reports just `{"batches": 0}`, as ever."""
        unmeasured = {job: section.unmeasured_fields() for job, section in self.polls.items()}
        out = self.model_dump(mode="json", by_alias=True, exclude={"polls": unmeasured})
        return {**out, "batches": {"batches": 0}} if self.batches is None else out

    def to_markdown(self) -> str:
        s, ss, b = self.station_observations, self.polls.get("station_status"), self.batches

        def polled(field: str) -> str:
            return str(getattr(ss, field, None))

        lines = [
            "# Data quality report, %s (UTC)" % self.day_utc, "",
            "**Verdict: %s**" % (self.verdict.value if self.verdict else "").upper(), "",
            "## Stream completeness",
            "- station_status polls logged: %s of %s expected (%s)" % (
                polled("polls_logged"), polled("expected_polls"), polled("completeness")),
            "- gaps over %ss: %s (total %ss)" % (self.thresholds.gap_threshold_s, polled("gaps_over_threshold"),
                                               polled("gap_seconds_total")),
            "- outcomes: %s" % polled("outcomes"), "",
            "## Observations",
            "- rows: %s across %s stations" % (s.observations, s.distinct_stations),
            "- stale share (threshold %s min): %s" % (self.thresholds.stale_minutes, s.stale_share),
            "- repeat share: %s, out-of-order: %s, unknown station rows: %s" % (
                s.repeat_share, s.out_of_order, s.unknown_station_rows),
            "- age CDF (seconds -> share of rows at or below): %s" % s.age_cdf, "",
            "## Pipeline",
            "- batches: %s, DLQ rows: %s, dropped delivery duplicates: %s" % (
                b.batches if b else 0, self.dlq.total, b.duplicates_dropped if b else None),
            "- latency (s): %s" % (b.latency_s.model_dump() if b else None),
            "- DLQ reasons: %s" % self.dlq.by_reason, "",
            "## Most frequently empty stations",
        ]
        lines += ["- %s: %s of observations, %s" % (t.station_id, t.empty_share, t.empty_observations)
                  for t in s.top_empty_stations]
        return "\n".join(lines) + "\n"
