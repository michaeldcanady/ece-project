"""One-shot health check of the lake: `citibike-status [--max-age-min 5]` (exit 1 when stale or empty)."""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, fields
from datetime import datetime
from typing import List, Optional, Tuple

from . import serving
from .batch_log import BatchLog
from .config import PipelineConfig
from .metrics import pct
from .storage import Storage
from .storage_ops import storage_from_config
from .timeutil import utcnow


@dataclass(frozen=True)
class LakeHealth:
    """What the serving files and the Batch log say about the lake."""
    last_batch_finished_min_ago: float
    data_as_of: datetime
    stations: int
    share_empty: float
    share_full: float
    share_stale: float
    at_risk_empty: int
    at_risk_full: int
    batches_last_24h: int
    dlq_rows_last_24h: int
    latency_p50_s_median_last_hour: Optional[float]


@dataclass(frozen=True)
class StatusReport:
    """The verdict of one health check: `health` is None when there are no serving files yet."""
    ok: bool
    health: Optional[LakeHealth]
    problem: Optional[str] = None

    def lines(self) -> List[Tuple[str, object]]:
        """The (name, value) lines `citibike-status` prints: ok, the health figures, then the problem if any."""
        out: List[Tuple[str, object]] = [("ok", self.ok)]
        if self.health is not None:
            out += [(f.name, _printable(getattr(self.health, f.name))) for f in fields(self.health)]
        if self.problem is not None:
            out.append(("problem", self.problem))
        return out


def _printable(value: object) -> object:
    return value.isoformat() if isinstance(value, datetime) else value


def summarize(storage: Storage, max_age_min: float) -> StatusReport:
    now = utcnow()
    batches = BatchLog(storage).recent(now)
    s = serving.read_summary(storage)
    if not batches or s is None:
        return StatusReport(ok=False, health=None,
                            problem="no serving files yet (is the micro-batch DAG running?)")
    last = max(batches, key=lambda r: r.finished_at)
    age_min = (now - last.finished_at).total_seconds() / 60.0
    p50 = [r.latencies.p50_s for r in batches[-60:] if r.latencies.p50_s is not None]
    health = LakeHealth(
        last_batch_finished_min_ago=round(age_min, 1), data_as_of=s.as_of, stations=s.n_stations,
        share_empty=s.share_empty, share_full=s.share_full, share_stale=s.share_stale,
        at_risk_empty=s.n_at_risk_empty, at_risk_full=s.n_at_risk_full, batches_last_24h=len(batches),
        dlq_rows_last_24h=sum(r.counts.dlq_rows for r in batches),
        latency_p50_s_median_last_hour=pct(sorted(p50), 0.5))
    ok = age_min <= max_age_min
    problem = None if ok else "last batch finished %.1f min ago (limit %s)" % (age_min, max_age_min)
    return StatusReport(ok=ok, health=health, problem=problem)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-age-min", type=float, default=5.0)
    args = ap.parse_args(argv)
    result = summarize(storage_from_config(PipelineConfig.from_env()), args.max_age_min)
    for k, v in result.lines():
        print("%-32s %s" % (k, v))
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
