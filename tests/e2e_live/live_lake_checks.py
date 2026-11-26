"""Checks 9-13: the data in the live lake itself, read only.

The cleaning checks (1-5) judge a lake this run built. These judge the lake the pipeline built over the last day
or more: its tables reconcile, hold no duplicates, agree with an independent reprocess of the same Kafka messages,
and the serving files agree with the history they were published from.
"""
from __future__ import annotations

from collections import defaultdict
from typing import List, Mapping, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from citibike_pipeline.storage import Storage, read_parquet

from .check_result import CheckResult
from .cleaning_checks import all_entries
from .lake_scan import read_all, read_files

Identity = Tuple[str, int, int]
_ID = ["kafka_topic", "kafka_partition", "kafka_offset"]
_COMPARED = ["num_bikes_available", "num_ebikes_available", "num_docks_available", "last_reported", "fetched_at",
             "station_name", "capacity"]


def _identities(table: pa.Table) -> List[Identity]:
    return list(zip(*(table[c].to_pylist() for c in _ID)))


def _duplicates(storage: Storage, prefix: str) -> Tuple[int, int]:
    """(duplicate identities outside any compacted hour, those inside one). The second kind is transient: a
    compacted file next to the batch files it merged, until the batch files are deleted."""
    files_of: Mapping[Identity, List[str]] = defaultdict(list)
    for key, table in read_files(storage, prefix, _ID):
        for ident in _identities(table):
            files_of[ident].append(key)
    stray = transient = 0
    for keys in files_of.values():
        if len(keys) > 1:
            if any(k.endswith("compacted.parquet") for k in keys):
                transient += 1
            else:
                stray += 1
    return stray, transient


def tables(storage: Storage) -> List[CheckResult]:
    """Reconcile raw, curated and dead letters, and check there are no stray duplicates."""
    raw = set(_identities(read_all(storage, "raw/station_status", _ID)))
    curated = set(_identities(read_all(storage, "curated/station_status", _ID)))
    rejected = set(_identities(read_all(storage, "dlq", _ID))) & raw
    dropped = sum(e.counts.duplicates_dropped for e in all_entries(storage))
    unexplained = len(raw) - len(curated) - len(rejected)
    out = [
        CheckResult("9a every curated station-status row comes from a raw row", curated <= raw,
                    "%d curated rows, %d without a raw row" % (len(curated), len(curated - raw))),
        CheckResult("9b raw = curated + parse-rejected + duplicates dropped", unexplained == dropped,
                    "raw %d, curated %d, rejected %d, unexplained %d, Batch log duplicates %d"
                    % (len(raw), len(curated), len(rejected), unexplained, dropped)),
    ]
    for prefix in ("raw/station_status", "curated/station_status", "curated/weather"):
        stray, transient = _duplicates(storage, prefix)
        out.append(CheckResult("10 no stray duplicate identity in %s" % prefix, stray == 0,
                               "%d stray; %d transient (compacted file beside its sources)" % (stray, transient)))
    return out


def _snapshot_capacities(storage: Storage) -> Mapping[str, set]:
    """Every capacity the station dimension held for each station, across its hourly snapshots."""
    caps: Mapping[str, set] = defaultdict(set)
    for key in storage.list("curated/dim_station/"):
        if key.endswith(".parquet"):
            for r in read_parquet(storage, key, columns=["station_id", "capacity"]).to_pylist():
                caps[r["station_id"]].add(r["capacity"])
    return caps


def agrees_with_reprocess(live: Storage, reprocessed: Storage) -> CheckResult:
    """For every message both lakes cleaned, the cleaned values are the same. Flags that depend on which batch a row
    fell in (repeat, out of order) are left out. The live lake joins the dimension as it stood when the batch ran,
    so its capacity may be any value the dimension held for that station, not just today's."""
    cols = _ID + ["station_id"] + _COMPARED
    a = read_all(live, "curated/station_status", cols)
    b = read_all(reprocessed, "curated/station_status", cols)
    exact = [c for c in _COMPARED if c != "capacity"]
    mine = {i: r for i, r in zip(_identities(b), zip(*(b[c].to_pylist() for c in ["station_id"] + _COMPARED)))}
    caps = _snapshot_capacities(live)
    overlap = differing = capacity_moved = 0
    first = ""
    for ident, row in zip(_identities(a), zip(*(a[c].to_pylist() for c in ["station_id"] + _COMPARED))):
        other = mine.get(ident)
        if other is None:
            continue
        overlap += 1
        names = ["station_id"] + _COMPARED
        bad = [n for n, x, y in zip(names, row, other) if x != y and n in exact + ["station_id"]]
        live_cap, their_cap = row[names.index("capacity")], other[names.index("capacity")]
        if live_cap != their_cap:
            if live_cap in caps[row[0]]:
                capacity_moved += 1
            else:
                bad.append("capacity")
        if bad:
            differing += 1
            first = first or "%s: %s differ, live %s vs reprocess %s" % (ident, bad, row, other)
    return CheckResult("11 live lake agrees with an independent reprocess of the same messages",
                       overlap > 0 and differing == 0,
                       "%d shared rows, %d differ, %d with a capacity the dimension changed since%s"
                       % (overlap, differing, capacity_moved, "; first " + first if first else ""))


def cleaning(storage: Storage, future_skew_s: int) -> List[CheckResult]:
    """Checks 3-5 on the live lake's tables."""
    from . import cleaning_checks
    out = [cleaning_checks.dead_letters(storage)] if storage.list("dlq/") else []
    out += cleaning_checks.value_sanity(storage, future_skew_s)
    out += cleaning_checks.station_dimension(storage)
    return [CheckResult("12 live lake: " + r.name, r.passed, r.detail) for r in out]


def serving_matches_history(storage: Storage) -> List[CheckResult]:
    """Read the serving files first, then the history: history only grows, so every published row must be in it."""
    state = read_parquet(storage, "gold/serving/current_station_state.parquet")
    summary = read_parquet(storage, "gold/serving/system_summary.parquet")
    cols = ["station_id", "fetched_at", "num_bikes_available", "num_ebikes_available", "num_docks_available",
            "last_reported"]
    hist = read_all(storage, "curated/station_status", cols)
    seen = {(s, f): (b, e, d, lr) for s, f, b, e, d, lr in zip(*(hist[c].to_pylist() for c in cols))}
    mismatched = missing = 0
    for r in state.to_pylist():
        got = seen.get((r["station_id"], r["observed_at"]))
        if got is None:
            missing += 1
        elif got != (r["num_bikes_available"], r["num_ebikes_available"], r["num_docks_available"],
                     r["last_reported"]):
            mismatched += 1
    n = summary.to_pylist()[0]["n_stations"]
    states = pc.unique(state["state"]).to_pylist()
    return [
        CheckResult("13a every served station state is a curated observation, with the same counts",
                    missing == 0 and mismatched == 0,
                    "%d stations served, %d not in history, %d with different counts"
                    % (state.num_rows, missing, mismatched)),
        CheckResult("13b the summary counts the stations served", n == state.num_rows,
                    "summary %d, served %d" % (n, state.num_rows)),
        CheckResult("13c served states are in the known set",
                    set(states) <= {"OK", "EMPTY", "FULL", "STALE", "OFFLINE"}, ", ".join(sorted(states))),
    ]
