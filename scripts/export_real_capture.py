"""Export a real capture from Kafka: the newest `--minutes` of station status, the weather and poll log around
it, and the latest snapshot of every station, as a gzip JSONL file `citibike_producer.replay` can replay.

Reads with its own consumer group and commits nothing, so the pipeline's offsets never move.

Usage: python scripts/export_real_capture.py [--minutes 45] [--out data/sample/real_capture.jsonl.gz]
"""
from __future__ import annotations

import argparse
import gzip
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Sequence

from citibike_contract import TOPIC_INFO, TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER, InvalidEnvelope, decode
from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.kafka_batch_reader import KafkaBatchReader
from citibike_pipeline.ranges import cap_ranges
from citibike_pipeline.record import Record

WEATHER_LOOKBACK = timedelta(hours=3)       # the as-of join wants the reading before the first status row
_TAIL_PER_MINUTE = 1500                     # messages per status partition to read back per window minute


@dataclass(frozen=True)
class CapturedLine:
    fetched_at: datetime
    topic: str
    value: bytes


def _lines(records: Sequence[Record]) -> List[CapturedLine]:
    out: List[CapturedLine] = []
    for rec in records:
        try:
            env = decode(rec.value)
        except InvalidEnvelope:
            continue
        out.append(CapturedLine(env.fetched_at, rec.topic, rec.value))
    return out


def select(lines: Sequence[CapturedLine], end: datetime, minutes: float) -> List[CapturedLine]:
    """The lines inside the window ending at `end`, oldest first; weather also from before the window starts."""
    start = end - timedelta(minutes=minutes)
    keep = [ln for ln in lines
            if (ln.topic == TOPIC_INFO)
            or (ln.topic == TOPIC_WEATHER and ln.fetched_at >= start - WEATHER_LOOKBACK)
            or (ln.topic in (TOPIC_STATUS, TOPIC_POLL_LOG) and ln.fetched_at >= start)]
    return sorted(keep, key=lambda ln: ln.fetched_at)


def _latest_per_key(records: Sequence[Record]) -> List[Record]:
    latest = {}
    for rec in records:                       # offset order: the last record of a key wins
        latest[rec.key] = rec
    return list(latest.values())


def export(out_path: str, minutes: float, cfg: PipelineConfig) -> int:
    reader = KafkaBatchReader(cfg.bootstrap_servers, "citibike-capture-export-%d" % int(time.time()))
    try:
        positions = reader.positions([TOPIC_STATUS, TOPIC_WEATHER, TOPIC_POLL_LOG, TOPIC_INFO], from_committed=False)
        tail = int(minutes * _TAIL_PER_MINUTE)
        ranges = cap_ranges({t: {p: (max(lo, hi - tail) if t == TOPIC_STATUS else lo, hi)
                                 for p, (lo, hi) in parts.items()} for t, parts in positions.items()})
        records = reader.read(ranges, cfg.read_timeout_s)
    finally:
        reader.close()
    info = _latest_per_key([r for r in records if r.topic == TOPIC_INFO])
    lines = _lines([r for r in records if r.topic != TOPIC_INFO]) + _lines(info)
    status = [ln for ln in lines if ln.topic == TOPIC_STATUS]
    if not status:
        raise SystemExit("no station_status messages to export")
    end = max(ln.fetched_at for ln in status)
    if min(ln.fetched_at for ln in status) > end - timedelta(minutes=minutes):
        raise SystemExit("the tail read covers less than %s minutes; raise _TAIL_PER_MINUTE" % minutes)
    chosen = select(lines, end, minutes)
    with open(out_path, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
        for ln in chosen:
            gz.write(ln.value.rstrip(b"\n") + b"\n")
    print("wrote %d envelopes (%d status) ending %s to %s"
          % (len(chosen), sum(1 for ln in chosen if ln.topic == TOPIC_STATUS), end.isoformat(), out_path))
    return len(chosen)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--minutes", type=float, default=45.0)
    ap.add_argument("--out", default="data/sample/real_capture.jsonl.gz")
    args = ap.parse_args(argv)
    export(args.out, args.minutes, PipelineConfig.from_env())
    return 0


if __name__ == "__main__":
    sys.exit(main())
