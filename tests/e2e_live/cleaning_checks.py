"""Checks 1-5: the cleaning is right, judged against what Kafka delivered, on the isolated lake."""
from __future__ import annotations

import json
from collections import Counter
from typing import List, Set, Tuple

import pyarrow.compute as pc

from citibike_pipeline.batch_log import BatchLog
from citibike_pipeline.ranges import Ranges
from citibike_pipeline.station_information_table import StationInformationTable
from citibike_pipeline.storage import Storage

from .check_result import CheckResult
from .lake_scan import read_all

Identity = Tuple[str, int, int]
_ID = ["kafka_topic", "kafka_partition", "kafka_offset"]


def _identities(table) -> List[Identity]:
    return list(zip(*(table[c].to_pylist() for c in _ID)))


def conservation(storage: Storage, targets: Ranges, batches_records: int) -> List[CheckResult]:
    """Every offset Kafka held at the start is in raw or in the dead-letter rows exactly once, and the Batch log
    counted the same messages."""
    raw = _identities(read_all(storage, "raw/station_status", _ID)) \
        + _identities(read_all(storage, "raw/weather", _ID)) \
        + _identities(read_all(storage, "raw/poll_log", _ID))
    raw_set = set(raw)
    dlq_all = _identities(read_all(storage, "dlq", _ID))
    dlq_only = set(dlq_all) - raw_set                       # rejected at the envelope: never reach raw
    seen = raw_set | dlq_only
    expected: Set[Identity] = {(t, p, o) for t, parts in targets.items() for p, (lo, hi) in parts.items()
                               for o in range(lo, hi)}
    missing = expected - seen
    dup_raw = len(raw) - len(raw_set)
    return [
        CheckResult("1a every Kafka message is in raw or the dead letters", not missing,
                    "%d expected offsets, %d missing" % (len(expected), len(missing))),
        CheckResult("1b no message is written to raw twice", dup_raw == 0, "%d duplicate identities" % dup_raw),
        CheckResult("1c Batch log read count equals the rows accounted for",
                    batches_records == len(seen),
                    "batch log read %d, raw+dlq cover %d" % (batches_records, len(seen))),
    ]


def status_balance(storage: Storage) -> CheckResult:
    """Raw station-status rows = curated rows + duplicate deliveries dropped + rows rejected by their parse."""
    raw = _identities(read_all(storage, "raw/station_status", _ID))
    curated = _identities(read_all(storage, "curated/station_status", _ID))
    rejected = set(_identities(read_all(storage, "dlq", _ID))) & set(raw)
    dropped = len(raw) - len(curated) - len(rejected)
    batch_dropped = sum(e.counts.duplicates_dropped for e in all_entries(storage))
    return CheckResult("1d raw = curated + parse-rejected + duplicates dropped", dropped == batch_dropped,
                       "raw %d, curated %d, rejected %d, unexplained-by-curated %d, Batch log duplicates %d"
                       % (len(raw), len(curated), len(rejected), dropped, batch_dropped))


def all_entries(storage: Storage):
    from datetime import datetime, timezone
    out = []
    days = {k.split("dt=")[1][:10] for k in storage.list("ops/batch_log/") if "dt=" in k}
    for d in sorted(days):
        day = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc).date()
        out.extend(BatchLog(storage).day(day))
    return out


def uniqueness(storage: Storage) -> List[CheckResult]:
    out = []
    for prefix in ("curated/station_status", "curated/weather"):
        ids = _identities(read_all(storage, prefix, _ID))
        out.append(CheckResult("2 no duplicate Kafka identity in %s" % prefix, len(ids) == len(set(ids)),
                               "%d rows, %d duplicates" % (len(ids), len(ids) - len(set(ids)))))
    return out


def dead_letters(storage: Storage) -> CheckResult:
    t = read_all(storage, "dlq", ["reason"])
    reasons = Counter(t["reason"].to_pylist())
    empty = sum(n for r, n in reasons.items() if not r)
    return CheckResult("3 every dead-letter row has a reason code", empty == 0,
                       "%d rows; %s" % (t.num_rows, json.dumps(dict(reasons.most_common(6)))))


def value_sanity(storage: Storage, future_skew_s: int) -> List[CheckResult]:
    t = read_all(storage, "curated/station_status",
                 ["num_bikes_available", "num_ebikes_available", "num_docks_available", "num_bikes_disabled",
                  "num_docks_disabled", "last_reported", "fetched_at"])
    out = []
    for c in ("num_bikes_available", "num_ebikes_available", "num_docks_available", "num_bikes_disabled",
              "num_docks_disabled"):
        bad = pc.sum(pc.less(t[c], 0).cast("int64")).as_py() or 0
        out.append(CheckResult("4 %s is never negative" % c, bad == 0, "%d of %d rows" % (bad, t.num_rows)))
    fetched_s = pc.divide(pc.cast(t["fetched_at"], "int64"), 1000)
    future = pc.sum(pc.greater(t["last_reported"], pc.add(fetched_s, future_skew_s)).cast("int64")).as_py() or 0
    out.append(CheckResult("4 last_reported is never in the future", future == 0,
                           "%d of %d rows" % (future, t.num_rows)))
    return out


def station_dimension(storage: Storage) -> List[CheckResult]:
    current = StationInformationTable().current(storage)
    if current is None:
        return [CheckResult("5a the station dimension is not empty", False, "the station dimension table is missing")]
    dim = current
    t = read_all(storage, "curated/station_status", ["is_unknown_station", "station_name"])
    unknown = pc.sum(t["is_unknown_station"].cast("int64")).as_py() or 0
    known_unnamed = pc.sum(pc.and_(pc.invert(t["is_unknown_station"]), pc.is_null(t["station_name"]))
                           .cast("int64")).as_py() or 0
    return [
        CheckResult("5a the station dimension is not empty", len(dim) > 0, "%d stations" % len(dim)),
        CheckResult("5b every known station got its dimension attributes",
                    known_unnamed == 0,
                    "%d unnamed known rows; %d of %d rows flagged unknown station"
                    % (known_unnamed, unknown, t.num_rows)),
    ]
