"""`mise run e2e:live`: reprocess Kafka's real messages into an isolated lake, check the cleaning and the live lake."""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import timezone
from typing import List, Optional

from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.local_storage import LocalStorage
from citibike_pipeline.timeutil import utcnow

from . import cleaning_checks, gold_checks, live_history_checks, live_lake_checks, serving_checks
from .check_result import CheckResult
from .report import render
from .reprocess import reprocess


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--live-lake", default=os.environ.get("CB_LOCAL_ROOT", "/lake"),
                    help="the running lake, read only (default CB_LOCAL_ROOT)")
    ap.add_argument("--work-dir", default=None, help="where the isolated lake is built (default: a new temp dir)")
    ap.add_argument("--max-batches", type=int, default=60)
    ap.add_argument("--report", default=None, help="also write the Markdown report to this path")
    args = ap.parse_args(argv)

    started = utcnow()
    cfg = PipelineConfig.from_env()
    work = args.work_dir or tempfile.mkdtemp(prefix="citibike-e2e-")
    print("reprocessing Kafka into %s ..." % work, flush=True)
    rp = reprocess(work, args.max_batches)
    live = LocalStorage(args.live_lake)

    entries = cleaning_checks.all_entries(rp.storage)
    results: List[CheckResult] = []
    results += cleaning_checks.conservation(rp.storage, rp.targets, sum(e.counts.records_read for e in entries))
    results.append(cleaning_checks.status_balance(rp.storage))
    results += cleaning_checks.uniqueness(rp.storage)
    results.append(cleaning_checks.dead_letters(rp.storage))
    results += cleaning_checks.value_sanity(rp.storage, cfg.future_skew_s)
    results += cleaning_checks.station_dimension(rp.storage)
    results += serving_checks.bi_readable(rp.storage, "reprocessed lake")
    results += serving_checks.bi_readable(live, "live lake")
    results += live_lake_checks.tables(live)
    results.append(live_lake_checks.agrees_with_reprocess(live, rp.storage))
    results += live_lake_checks.cleaning(live, cfg.future_skew_s)
    results += live_lake_checks.serving_matches_history(live)
    results += gold_checks.gold(live, cfg)
    results.append(live_history_checks.latency(live))
    results += live_history_checks.continuity(live, cfg)

    notes = ["Reprocessed %d messages in %d batches into %s (consumer group %s); the live lake was only read."
             % (rp.messages, rp.batches, work, rp.cfg.consumer_group),
             "Qlik's refresh is not checked here (manual)."]
    text = render(results, started.astimezone(timezone.utc), notes)
    print(text)
    if args.report:
        with open(args.report, "w") as f:
            f.write(text)
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
