"""Daily data quality report (UTC day). Streams one hour at a time to bound memory."""
from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.compute as pc

from . import lake
from .config import PipelineConfig
from .metrics import pct
from .storage import Storage, put_json, read_parquet
from .timeutil import epoch, from_epoch, iso, partition_path

log = logging.getLogger(__name__)

AGE_EDGES_S = [60, 300, 900, 1800, 3600, 7200, 21600]     # for the stale-threshold distribution
STATUS_COLS = lake.IDENTITY + ["station_id", "fetched_at", "age_s", "is_stale", "is_repeat", "is_out_of_order",
                               "is_unknown_station", "num_bikes_available", "is_installed", "is_renting",
                               "is_returning"]


def _day_bounds(day: date):
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _poll_log_section(storage: Storage, cfg: PipelineConfig, start: datetime, end: datetime) -> Dict[str, Any]:
    t = lake.load_window(storage, "raw/poll_log", start, end, lake.IDENTITY + ["fetched_at", "payload_json"])
    by_job: Dict[str, List[Dict[str, Any]]] = {}
    for fetched, payload in zip(t["fetched_at"].to_pylist(), t["payload_json"].to_pylist()):
        p = json.loads(payload)
        by_job.setdefault(p.get("job", "?"), []).append({"t": epoch(fetched), "p": p})

    out: Dict[str, Any] = {}
    for job, items in sorted(by_job.items()):
        items.sort(key=lambda x: x["t"])
        outcomes = Counter(i["p"].get("outcome") for i in items)
        lat = sorted(i["p"]["http_latency_ms"] for i in items if i["p"].get("http_latency_ms") is not None)
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


def _status_section(storage: Storage, start: datetime) -> Dict[str, Any]:
    rows = stale = repeats = ooo = unknown = 0
    stations = set()
    age_le = Counter()                       # edge -> rows with age <= edge
    empty_by_station: Counter = Counter()
    obs_by_station: Counter = Counter()
    empty_by_hour: Counter = Counter()
    obs_by_hour: Counter = Counter()
    for h in range(24):
        hs = start + timedelta(hours=h)
        t = lake.load_window(storage, lake.CURATED_STATUS, hs, hs + timedelta(hours=1), STATUS_COLS)
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
    return {
        "observations": rows, "distinct_stations": len(stations),
        "stale_share": round(stale / float(rows), 4) if rows else None,
        "repeat_share": round(repeats / float(rows), 4) if rows else None,
        "out_of_order": ooo, "unknown_station_rows": unknown,
        "age_cdf": {str(e): round(age_le[e] / float(rows), 4) for e in AGE_EDGES_S} if rows else {},
        "top_empty_stations": top,
        "empty_share_by_hour_utc": {str(h): round(empty_by_hour[h] / float(obs_by_hour[h]), 4)
                                    for h in sorted(obs_by_hour)},
    }


def _batch_section(storage: Storage, start: datetime) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for h in range(24):
        for key in storage.list(partition_path(lake.BATCH_LOG, start + timedelta(hours=h)) + "/"):
            if key.endswith(".parquet"):
                rows.extend(read_parquet(storage, key).to_pylist())
    if not rows:
        return {"batches": 0}
    p50 = sorted(r["latency_p50_s"] for r in rows if r["latency_p50_s"] is not None)
    p95 = sorted(r["latency_p95_s"] for r in rows if r["latency_p95_s"] is not None)
    return {
        "batches": len(rows), "records_read": sum(r["records_read"] for r in rows),
        "dlq_rows": sum(r["dlq_rows"] for r in rows), "duplicates_dropped": sum(r["duplicates_dropped"] for r in rows),
        "replay_rows": sum(r["replay_rows"] for r in rows),
        "latency_s": {"median_of_batch_p50": pct(p50, 0.5), "p95_of_batch_p95": pct(p95, 0.95),
                      "max": max((r["latency_max_s"] for r in rows if r["latency_max_s"] is not None), default=None)},
    }


def _dlq_section(storage: Storage, start: datetime) -> Dict[str, Any]:
    reasons: Counter = Counter()
    for h in range(24):
        for key in storage.list(partition_path(lake.DLQ, start + timedelta(hours=h)) + "/"):
            if key.endswith(".parquet"):
                reasons.update(read_parquet(storage, key, columns=["reason"])["reason"].to_pylist())
    return {"total": sum(reasons.values()), "by_reason": dict(reasons.most_common())}


def verdict(report: Dict[str, Any]) -> str:
    status = report["station_observations"]
    poll = report["polls"].get("station_status", {})
    if not status["observations"]:
        return "fail"
    completeness = poll.get("completeness", 0)
    dlq_rate = report["dlq"]["total"] / float(max(report["batches"].get("records_read", 0), 1))
    if completeness < 0.5:
        return "fail"
    if completeness < 0.95 or (status["stale_share"] or 0) > 0.05 or dlq_rate > 0.01:
        return "warn"
    return "ok"


def daily_quality(storage: Storage, cfg: PipelineConfig, day: date) -> Dict[str, Any]:
    start, end = _day_bounds(day)
    report = {
        "day_utc": day.isoformat(), "thresholds": {"stale_minutes": cfg.stale_minutes,
                                                   "gap_threshold_s": cfg.gap_threshold_s},
        "polls": _poll_log_section(storage, cfg, start, end),
        "station_observations": _status_section(storage, start),
        "batches": _batch_section(storage, start),
        "dlq": _dlq_section(storage, start),
    }
    report["verdict"] = verdict(report)
    log.info("daily quality %s: verdict=%s", day.isoformat(), report["verdict"])
    base = "quality/daily/dt=%s" % day.isoformat()
    put_json(storage, base + "/report.json", report)
    storage.put(base + "/report.md", render_markdown(report).encode("utf-8"))
    return report


def render_markdown(r: Dict[str, Any]) -> str:
    s, ss = r["station_observations"], r["polls"].get("station_status", {})
    lines = [
        "# Data quality report, %s (UTC)" % r["day_utc"], "", "**Verdict: %s**" % r["verdict"].upper(), "",
        "## Stream completeness",
        "- station_status polls logged: %s of %s expected (%s)" % (
            ss.get("polls_logged"), ss.get("expected_polls"), ss.get("completeness")),
        "- gaps over %ss: %s (total %ss)" % (r["thresholds"]["gap_threshold_s"], ss.get("gaps_over_threshold"),
                                           ss.get("gap_seconds_total")),
        "- outcomes: %s" % ss.get("outcomes"), "",
        "## Observations",
        "- rows: %s across %s stations" % (s["observations"], s["distinct_stations"]),
        "- stale share (threshold %s min): %s" % (r["thresholds"]["stale_minutes"], s["stale_share"]),
        "- repeat share: %s, out-of-order: %s, unknown station rows: %s" % (
            s["repeat_share"], s["out_of_order"], s["unknown_station_rows"]),
        "- age CDF (seconds -> share of rows at or below): %s" % s["age_cdf"], "",
        "## Pipeline",
        "- batches: %s, DLQ rows: %s, dropped delivery duplicates: %s" % (
            r["batches"].get("batches"), r["dlq"]["total"], r["batches"].get("duplicates_dropped")),
        "- latency (s): %s" % r["batches"].get("latency_s"),
        "- DLQ reasons: %s" % r["dlq"]["by_reason"], "",
        "## Most frequently empty stations",
    ]
    lines += ["- %s: %s of observations, %s" % (t["station_id"], t["empty_share"], t["empty_observations"])
              for t in s["top_empty_stations"]]
    return "\n".join(lines) + "\n"
