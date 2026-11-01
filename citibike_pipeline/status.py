"""One-shot health check of the lake: `citibike-status [--max-age-min 5]` (exit 1 when stale or empty)."""
from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional

from . import lake
from .config import PipelineConfig
from .metrics import pct
from .storage import Storage, read_parquet, storage_from_config
from .timeutil import utcnow


def summarize(storage: Storage, max_age_min: float) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": False}
    if not storage.exists(lake.SERVING_BATCH_LOG) or not storage.exists(lake.SERVING_SUMMARY):
        out["problem"] = "no serving files yet (is the micro-batch DAG running?)"
        return out
    batches = read_parquet(storage, lake.SERVING_BATCH_LOG).to_pylist()
    last = max(batches, key=lambda r: r["finished_at"])
    age_min = (utcnow() - last["finished_at"]).total_seconds() / 60.0
    s = read_parquet(storage, lake.SERVING_SUMMARY).to_pylist()[0]
    p50 = [r["latency_p50_s"] for r in batches[-60:] if r["latency_p50_s"] is not None]
    out.update({
        "last_batch_finished_min_ago": round(age_min, 1), "data_as_of": s["as_of"].isoformat(),
        "stations": s["n_stations"], "share_empty": s["share_empty"], "share_full": s["share_full"],
        "share_stale": s["share_stale"], "at_risk_empty": s["n_at_risk_empty"], "at_risk_full": s["n_at_risk_full"],
        "batches_last_24h": len(batches), "dlq_rows_last_24h": sum(r["dlq_rows"] for r in batches),
        "latency_p50_s_median_last_hour": pct(sorted(p50), 0.5),
    })
    out["ok"] = age_min <= max_age_min
    if not out["ok"]:
        out["problem"] = "last batch finished %.1f min ago (limit %s)" % (age_min, max_age_min)
    return out


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-age-min", type=float, default=5.0)
    args = ap.parse_args(argv)
    result = summarize(storage_from_config(PipelineConfig.from_env()), args.max_age_min)
    for k, v in result.items():
        print("%-32s %s" % (k, v))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
