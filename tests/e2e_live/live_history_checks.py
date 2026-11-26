"""Checks 7-8, read-only on the live lake: latency against the 2-minute target, and poll continuity."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import List

from citibike_contract import InvalidPollLog, PollLog
from citibike_pipeline import lake
from citibike_pipeline.batch_log import BatchLog
from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.metrics import pct
from citibike_pipeline.storage import Storage
from citibike_pipeline.timeutil import utcnow

from .check_result import CheckResult

LATENCY_TARGET_S = 120.0
_HEALTHY = frozenset({"ok", "skipped_unchanged"})


def _days(storage: Storage, prefix: str) -> List[datetime]:
    names = {k.split("dt=")[1][:10] for k in storage.list(prefix + "/") if "dt=" in k}
    return [datetime.strptime(n, "%Y-%m-%d").replace(tzinfo=timezone.utc) for n in sorted(names)]


def latency(storage: Storage) -> CheckResult:
    entries = []
    for day in _days(storage, "ops/batch_log"):
        entries.extend(BatchLog(storage).day(day.date()))
    p95s = sorted(e.latencies.p95_s for e in entries if e.latencies.p95_s is not None)
    if not p95s:
        return CheckResult("7 latency p95 under %ds" % LATENCY_TARGET_S, False, "no batches with a latency")
    value = pct(p95s, 0.95)
    return CheckResult("7 latency: 95th percentile of batch p95 under %ds" % LATENCY_TARGET_S, value < LATENCY_TARGET_S,
                       "%d batches; p95 of batch p95 = %.1fs, median batch p95 = %.1fs, max = %.1fs"
                       % (len(p95s), value, pct(p95s, 0.5), p95s[-1]))


def continuity(storage: Storage, cfg: PipelineConfig) -> List[CheckResult]:
    days = _days(storage, "raw/poll_log")
    if not days:
        return [CheckResult("8 poll continuity", False, "no poll log in the lake")]
    start, end = days[0], days[-1] + timedelta(days=1)
    t = lake.RAW_POLL_LOG.read_window(storage, start, min(end, utcnow() + timedelta(minutes=1)),
                                      ["fetched_at", "payload_json"])
    polls = []
    for fetched, payload in zip(t["fetched_at"].to_pylist(), t["payload_json"].to_pylist()):
        try:
            p = PollLog.from_payload(json.loads(payload))
        except (InvalidPollLog, ValueError):
            continue
        if p.job == "station_status":
            polls.append((fetched.timestamp(), p.outcome))
    polls.sort()
    gaps = [(a, b) for (a, _), (b, _) in zip(polls, polls[1:]) if b - a > cfg.gap_threshold_s]
    outcomes = Counter(o for _, o in polls)
    span_h = (polls[-1][0] - polls[0][0]) / 3600.0 if len(polls) > 1 else 0.0
    failed = sum(n for o, n in outcomes.items() if o not in _HEALTHY)
    worst = max((b - a for a, b in gaps), default=0.0)
    observed = (polls[-1][0] - polls[0][0]) / 86400.0 if len(polls) > 1 else 0.0
    return [
        CheckResult("8a no silence over %ds between logged status polls" % cfg.gap_threshold_s, not gaps,
                    "%d polls logged over %.1fh; %d gaps, worst %.0fs" % (len(polls), span_h, len(gaps), worst)),
        CheckResult("8b at most 5% of status polls failed (an unchanged feed is not a failure)",
                    failed <= 0.05 * len(polls),
                    "%d failed of %d; %s" % (failed, len(polls), json.dumps(dict(outcomes)))),
        CheckResult("8c days observed (the proposal targets 7)", observed >= 7.0,
                    "%.1f days of poll history" % observed),
    ]
