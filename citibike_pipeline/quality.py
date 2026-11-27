"""Daily data quality report (UTC day). Streams one hour at a time to bound memory."""
from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional

import pyarrow as pa
import pyarrow.compute as pc
from pydantic import BaseModel, Field

from citibike_contract import InvalidPollLog, PollLog

from . import lake
from .batch_log import BatchLog
from .config import PipelineConfig
from .metrics import pct
from .storage import Storage, put_json, read_parquet
from .timeutil import epoch, from_epoch, iso

log = logging.getLogger(__name__)

AGE_EDGES_S = [60, 300, 900, 1800, 3600, 7200, 21600]     # for the stale-threshold distribution
STATUS_COLS = ["station_id", "fetched_at", "age_s", "is_stale", "is_repeat", "is_out_of_order",
               "is_unknown_station", "num_bikes_available", "is_installed", "is_renting", "is_returning"]


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _poll_log_section(storage: Storage, cfg: PipelineConfig, start: datetime, end: datetime) -> Dict[str, Any]:
    t = lake.RAW_POLL_LOG.read_window(storage, start, end, ["fetched_at", "payload_json"])
    by_job: Dict[str, List[Dict[str, Any]]] = {}
    for fetched, payload in zip(t["fetched_at"].to_pylist(), t["payload_json"].to_pylist()):
        try:
            p = PollLog.from_payload(json.loads(payload))
        except (InvalidPollLog, ValueError) as exc:
            log.warning("skipping an undecodable poll log at %s: %s", iso(fetched), exc)
            continue
        by_job.setdefault(p.job, []).append({"t": epoch(fetched), "p": p})

    out: Dict[str, Any] = {}
    for job, items in sorted(by_job.items()):
        items.sort(key=lambda x: x["t"])
        outcomes = Counter(i["p"].outcome for i in items)
        lat = sorted(i["p"].http_latency_ms for i in items if i["p"].http_latency_ms is not None)
        section: Dict[str, Any] = {
            "polls_logged": len(items), "outcomes": dict(outcomes),
            "http_latency_ms_p50": pct(lat, 0.5), "http_latency_ms_p95": pct(lat, 0.95),
        }
        if job == "station_status":
            expected = int(86400 / cfg.status_interval_s)
            gaps = [{"from": iso(from_epoch(a["t"])), "to": iso(from_epoch(b["t"])),
                     "seconds": round(b["t"] - a["t"], 1)}
                    for a, b in zip(items, items[1:]) if b["t"] - a["t"] > cfg.gap_threshold_s]
            section.update({
                "expected_polls": expected, "completeness": round(len(items) / float(expected), 4),
                "gaps_over_threshold": len(gaps), "gap_seconds_total": round(sum(g["seconds"] for g in gaps), 1),
                "largest_gaps": sorted(gaps, key=lambda g: -g["seconds"])[:10],
            })
        out[job] = section
    return out


class SectionStatus(BaseModel):
    observations: int
    distinct_stations: int
    stale_share: Optional[float]
    repeat_share: Optional[float]
    out_of_order: int
    unknown_station_rows: int
    age_cdf: Dict[str, float]
    top_empty_stations: List[Dict[str, Any]]
    empty_share_by_hour_utc: Dict[str, float]

def _status_section(storage: Storage, start: datetime) -> SectionStatus:
    rows = stale = repeats = ooo = unknown = 0
    stations = set()
    age_le: Counter = Counter()                       # edge -> rows with age <= edge
    empty_by_station: Counter = Counter()
    obs_by_station: Counter = Counter()
    empty_by_hour: Counter = Counter()
    obs_by_hour: Counter = Counter()
    for h in range(24):
        hs = start + timedelta(hours=h)
        t = lake.CURATED_STATUS.read_window(storage, hs, hs + timedelta(hours=1), STATUS_COLS)
        if t.num_rows == 0:
            continue
        rows += t.num_rows
        stale += pc.sum(t["is_stale"].cast(pa.int64())).as_py() or 0
        repeats += pc.sum(t["is_repeat"].cast(pa.int64())).as_py() or 0
        ooo += pc.sum(t["is_out_of_order"].cast(pa.int64())).as_py() or 0
        unknown += pc.sum(t["is_unknown_station"].cast(pa.int64())).as_py() or 0
        stations.update(pc.unique(t["station_id"]).to_pylist())
        for e in AGE_EDGES_S:
            age_le[e] += pc.sum(pc.less_equal(t["age_s"], e).cast(pa.int64())).as_py() or 0
        operational = pc.and_(pc.and_(t["is_installed"], t["is_renting"]), t["is_returning"])
        empty = pc.and_(pc.and_(operational, pc.equal(t["num_bikes_available"], 0)), pc.invert(t["is_stale"]))
        ids = t["station_id"].to_pylist()
        obs_by_station.update(ids)
        e_ids = t.filter(empty)["station_id"].to_pylist()
        empty_by_station.update(e_ids)
        obs_by_hour[h] += t.num_rows
        empty_by_hour[h] += len(e_ids)

    top = [{"station_id": sid, "empty_share": round(n / float(obs_by_station[sid]), 4), "empty_observations": n}
           for sid, n in empty_by_station.most_common(10)]
    return SectionStatus(
        observations=rows,
        distinct_stations=len(stations),
        stale_share=round(stale / float(rows), 4) if rows else None,
        repeat_share=round(repeats / float(rows), 4) if rows else None,
        out_of_order=ooo,
        unknown_station_rows=unknown,
        age_cdf={str(e): round(age_le[e] / float(rows), 4) for e in AGE_EDGES_S} if rows else {},
        top_empty_stations=top,
        empty_share_by_hour_utc={str(h): round(empty_by_hour[h] / float(obs_by_hour[h]), 4)
                                 for h in sorted(obs_by_hour)},
    )


class BatchLatency(BaseModel):
    median_of_batch_p50: Optional[float] = None
    p95_of_batch_p95: Optional[float] = None
    max: Optional[float] = None


class BatchSection(BaseModel):
    """The day's Batch log entries, summed. Only built when the day had at least one batch."""
    batches: int = 0
    records_read: int = 0
    dlq_rows: int = 0
    duplicates_dropped: int = 0
    replay_rows: int = 0
    latency_s: BatchLatency = Field(default_factory=BatchLatency)


def _batch_section(storage: Storage, start: datetime) -> Optional[BatchSection]:
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


class DLQSection(BaseModel):
    total: int
    by_reason: Dict[str, int]

def _dlq_section(storage: Storage, start: datetime) -> DLQSection:
    reasons: Counter = Counter()
    for h in range(24):
        for key in storage.list(lake.partition_path(lake.DLQ, start + timedelta(hours=h)) + "/"):
            if key.endswith(".parquet"):
                reasons.update(read_parquet(storage, key, columns=["reason"])["reason"].to_pylist())
    return DLQSection(total=sum(reasons.values()), by_reason=dict(reasons.most_common()))


class Verdict(Enum):
    OK = "ok"
    WARN = "warn"
    FAIL = "fail"

def verdict(report: DailyQualityReport) -> Verdict:
    status = report.station_observations
    poll = report.polls.get("station_status", {})
    if not status.observations:
        return Verdict.FAIL
    completeness = poll.get("completeness", 0)
    records_read = report.batches.records_read if report.batches else 0
    dlq_rate = report.dlq.total / float(max(records_read, 1))
    if completeness < 0.5:
        return Verdict.FAIL
    if completeness < 0.95 or (status.stale_share or 0) > 0.05 or dlq_rate > 0.01:
        return Verdict.WARN
    return Verdict.OK

class DailyQualityReport(BaseModel):
    day_utc: str
    thresholds: DailyQualityReportThresholds
    polls: Dict[str, Any]
    station_observations: SectionStatus
    batches: Optional[BatchSection]
    dlq: DLQSection
    verdict: Optional[Verdict] = None

    def to_json(self) -> Mapping[str, object]:
        """The report.json object: a day with no batches reports just `{"batches": 0}`, as ever."""
        out = self.model_dump(mode="json")
        return {**out, "batches": {"batches": 0}} if self.batches is None else out


class DailyQualityReportThresholds(BaseModel):
    stale_minutes: float
    gap_threshold_s: int

def daily_quality(storage: Storage, cfg: PipelineConfig, day: date) -> DailyQualityReport:
    start, end = _day_bounds(day)
    batches = _batch_section(storage, start)
    report = DailyQualityReport(
        day_utc=day.isoformat(),
        thresholds=DailyQualityReportThresholds(stale_minutes=cfg.stale_minutes, gap_threshold_s=cfg.gap_threshold_s),
        polls=_poll_log_section(storage, cfg, start, end),
        station_observations=_status_section(storage, start),
        batches=batches,
        dlq=_dlq_section(storage, start),
    )
    report.verdict = verdict(report)
    log.info("daily quality %s: verdict=%s", day.isoformat(), report.verdict)
    base = "quality/daily/dt=%s" % day.isoformat()
    put_json(storage, base + "/report.json", report.to_json())
    storage.put(base + "/report.md", render_markdown(report).encode("utf-8"))
    return report


def render_markdown(r: DailyQualityReport) -> str:
    s, ss, b = r.station_observations, r.polls.get("station_status", {}), r.batches
    lines = [
        "# Data quality report, %s (UTC)" % r.day_utc, "",
        "**Verdict: %s**" % (r.verdict.value if r.verdict else "").upper(), "",
        "## Stream completeness",
        "- station_status polls logged: %s of %s expected (%s)" % (
            ss.get("polls_logged"), ss.get("expected_polls"), ss.get("completeness")),
        "- gaps over %ss: %s (total %ss)" % (r.thresholds.gap_threshold_s, ss.get("gaps_over_threshold"),
                                           ss.get("gap_seconds_total")),
        "- outcomes: %s" % ss.get("outcomes"), "",
        "## Observations",
        "- rows: %s across %s stations" % (s.observations, s.distinct_stations),
        "- stale share (threshold %s min): %s" % (r.thresholds.stale_minutes, s.stale_share),
        "- repeat share: %s, out-of-order: %s, unknown station rows: %s" % (
            s.repeat_share, s.out_of_order, s.unknown_station_rows),
        "- age CDF (seconds -> share of rows at or below): %s" % s.age_cdf, "",
        "## Pipeline",
        "- batches: %s, DLQ rows: %s, dropped delivery duplicates: %s" % (
            b.batches if b else 0, r.dlq.total, b.duplicates_dropped if b else None),
        "- latency (s): %s" % (b.latency_s.model_dump() if b else None),
        "- DLQ reasons: %s" % r.dlq.by_reason, "",
        "## Most frequently empty stations",
    ]
    lines += ["- %s: %s of observations, %s" % (t["station_id"], t["empty_share"], t["empty_observations"])
              for t in s.top_empty_stations]
    return "\n".join(lines) + "\n"
