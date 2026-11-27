"""Checks 14-16: the gold tables follow the metric rules and agree with the curated rows they came from.

Rules are those of `citibike_pipeline.window_metrics` and `snapshot`: a window holds the observations with
as_of - 15 min < fetched_at <= as_of; a rate needs two observations and at least the minimum coverage; a station is
at risk only when its time to empty or full is within the horizon and it moved at least the noise floor.
The recompute reads the curated rows and applies the window rule itself, not the pipeline's functions.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from typing import List, Mapping, Optional, Tuple

from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.storage import Storage
from citibike_pipeline.storage_ops import read_parquet

from .check_result import CheckResult
from .lake_scan import read_all

_WINDOW_COLS = ["station_id", "as_of", "window_start", "n_obs", "coverage_min", "bikes_first", "bikes_last",
                "docks_first", "docks_last", "net_change", "rate_per_min", "docks_rate_per_min",
                "minutes_to_empty", "minutes_to_full", "at_risk_empty", "at_risk_full", "batch_id"]
_SAMPLE_BATCHES = 20
_SKIP_NEWEST = 3        # the newest batches can still be waiting for late rows


def _close(a: Optional[float], b: Optional[float]) -> bool:
    if a is None or b is None:
        return a is b
    return abs(a - b) <= 1e-6 * max(1.0, abs(b))


def _time_to_zero(count: int, rate: Optional[float]) -> Optional[float]:
    if count == 0:
        return 0.0
    return count / -rate if rate is not None and rate < 0 else None


def window_row_problems(r: Mapping[str, object], cfg: PipelineConfig) -> List[str]:
    """What is wrong with one gold window row, judged by the rules alone (empty when it is consistent)."""
    bad: List[str] = []
    n, cov = int(r["n_obs"]), float(r["coverage_min"])
    first_b, last_b = int(r["bikes_first"]), int(r["bikes_last"])
    first_d, last_d = int(r["docks_first"]), int(r["docks_last"])
    rate, drate = r["rate_per_min"], r["docks_rate_per_min"]
    if n < 1 or cov < 0:
        bad.append("n_obs or coverage out of range")
    if int(r["net_change"]) != last_b - first_b:
        bad.append("net_change is not bikes_last - bikes_first")
    if (rate is not None) != (n >= 2 and cov >= cfg.min_coverage_minutes):
        bad.append("a rate is present only with 2+ observations and enough coverage")
    elif rate is not None:
        if not _close(float(rate), (last_b - first_b) / cov):
            bad.append("rate_per_min is not net change over coverage")
        if drate is None or not _close(float(drate), (last_d - first_d) / cov):
            bad.append("docks_rate_per_min is not the dock change over coverage")
    mte = _time_to_zero(last_b, None if rate is None else float(rate))
    mtf = _time_to_zero(last_d, None if drate is None else float(drate))
    if not _close(r["minutes_to_empty"], mte):
        bad.append("minutes_to_empty does not follow from bikes_last and the rate")
    if not _close(r["minutes_to_full"], mtf):
        bad.append("minutes_to_full does not follow from docks_last and the dock rate")
    risk_e = last_b > 0 and mte is not None and mte <= cfg.risk_horizon_minutes \
        and (first_b - last_b) >= cfg.noise_floor_bikes
    risk_f = last_d > 0 and mtf is not None and mtf <= cfg.risk_horizon_minutes \
        and (first_d - last_d) >= cfg.noise_floor_bikes
    if bool(r["at_risk_empty"]) != risk_e or bool(r["at_risk_full"]) != risk_f:
        bad.append("at-risk flags do not follow the horizon and noise-floor rule")
    return bad


def window_rules(windows, cfg: PipelineConfig) -> List[CheckResult]:
    rows = windows.to_pylist()
    problems = defaultdict(int)
    first = ""
    for r in rows:
        for p in window_row_problems(r, cfg):
            problems[p] += 1
            first = first or "%s at %s: %s" % (r["station_id"], r["as_of"], p)
    keys = [(r["station_id"], r["as_of"], r["batch_id"]) for r in rows]
    stray = len(keys) - len(set(keys))
    return [
        CheckResult("14a every gold window row follows the metric rules", not problems,
                    "%d rows; %s%s" % (len(rows), dict(problems) or "no violations",
                                       "; first " + first if first else "")),
        CheckResult("14b no duplicate (station, as_of, batch) window row", stray == 0, "%d duplicates" % stray),
    ]


def _observations(storage: Storage) -> Mapping[str, Tuple[List[float], List[Tuple[int, int]]]]:
    cols = ["station_id", "fetched_at", "kafka_partition", "kafka_offset", "num_bikes_available", "num_docks_available"]
    t = read_all(storage, "curated/station_status", cols)
    per = defaultdict(list)
    for sid, f, p, o, b, d in zip(*(t[c].to_pylist() for c in cols)):
        per[sid].append((f.timestamp(), p, o, b, d))
    out = {}
    for sid, items in per.items():
        items.sort()
        out[sid] = ([i[0] for i in items], [(i[3], i[4]) for i in items])
    return out


def recompute_windows(storage: Storage, windows, cfg: PipelineConfig) -> CheckResult:
    """For a spread of recent batches, rebuild each station's window from the curated rows and compare."""
    rows = windows.to_pylist()
    batches = sorted({(r["as_of"], r["batch_id"]) for r in rows})
    usable = batches[:-_SKIP_NEWEST] if len(batches) > _SKIP_NEWEST else []
    if not usable:
        return CheckResult("15 gold windows recompute from the curated rows", False, "too few batches to sample")
    step = max(1, len(usable) // _SAMPLE_BATCHES)
    chosen = set(usable[::step][-_SAMPLE_BATCHES:])
    obs = _observations(storage)
    checked = differing = 0
    first = ""
    for r in rows:
        if (r["as_of"], r["batch_id"]) not in chosen:
            continue
        times, values = obs.get(r["station_id"], ([], []))
        as_of = r["as_of"].timestamp()
        lo, hi = bisect_right(times, as_of - cfg.window_minutes * 60), bisect_right(times, as_of)
        got = (hi - lo, values[lo][0] if hi > lo else None, values[hi - 1][0] if hi > lo else None,
               values[lo][1] if hi > lo else None, values[hi - 1][1] if hi > lo else None,
               (times[hi - 1] - times[lo]) / 60.0 if hi > lo else None)
        want = (r["n_obs"], r["bikes_first"], r["bikes_last"], r["docks_first"], r["docks_last"], r["coverage_min"])
        checked += 1
        if not (got[:5] == want[:5] and _close(got[5], want[5])):
            differing += 1
            first = first or "%s at %s: curated %s vs gold %s" % (r["station_id"], r["as_of"], got, want)
    return CheckResult("15 gold windows recompute from the curated rows", differing == 0,
                       "%d window rows over %d sampled batches, %d differ%s"
                       % (checked, len(chosen), differing, "; first " + first if first else ""))


def _served_matches_window(s: Mapping[str, object], w: Mapping[str, object]) -> bool:
    """A served state repeats its window's figures, and is flagged at risk only when its state is OK."""
    flag = ("EMPTY" if w["at_risk_empty"] else "FULL" if w["at_risk_full"] else None) if s["state"] == "OK" else None
    return s["net_change_15m"] == w["net_change"] and _close(s["rate_per_min"], w["rate_per_min"]) \
        and s["at_risk"] == flag


def serving_follows_gold(storage: Storage) -> List[CheckResult]:
    """Serving files are read before the gold windows: history only grows, so a batch published meanwhile is in it."""
    state = read_parquet(storage, "gold/serving/current_station_state.parquet").to_pylist()
    at_risk = read_parquet(storage, "gold/serving/at_risk.parquet").to_pylist()
    summary = read_parquet(storage, "gold/serving/system_summary.parquet").to_pylist()[0]
    windows = read_all(storage, "gold/station_window_15m", _WINDOW_COLS)
    by_key = {(w["station_id"], w["batch_id"]): w for w in windows.to_pylist()}

    def count(pred) -> int:
        return sum(1 for s in state if pred(s))

    operational = [s for s in state if s["state"] != "OFFLINE"]
    n_op, n = len(operational) or 1, len(state)
    expected = {
        "n_stations": n, "n_operational": len(operational),
        "share_with_bike": sum(1 for s in operational if s["num_bikes_available"] > 0) / float(n_op),
        "share_with_dock": sum(1 for s in operational if s["num_docks_available"] > 0) / float(n_op),
        "share_empty": count(lambda s: s["state"] == "EMPTY") / float(n),
        "share_full": count(lambda s: s["state"] == "FULL") / float(n),
        "share_stale": count(lambda s: s["state"] == "STALE") / float(n),
        "share_offline": count(lambda s: s["state"] == "OFFLINE") / float(n),
        "n_at_risk_empty": count(lambda s: s["at_risk"] == "EMPTY"),
        "n_at_risk_full": count(lambda s: s["at_risk"] == "FULL"),
    }
    off = [k for k, v in expected.items() if not _close(summary[k], v)]
    risky = [s for s in state if s["at_risk"]]
    listed = {s["station_id"] for s in at_risk}
    urgency = [min(x for x in (s["minutes_to_empty"], s["minutes_to_full"]) if x is not None) for s in at_risk]
    wrong_window = 0
    for s in state:
        w = by_key.get((s["station_id"], s["batch_id"]))
        if w is None:
            wrong_window += 0 if s["net_change_15m"] is None and s["at_risk"] is None else 1
        elif not _served_matches_window(s, w):
            wrong_window += 1
    return [
        CheckResult("16a the system summary equals the shares and counts recomputed from the served states", not off,
                    "differs on %s" % off if off else "%d stations, all %d figures match" % (n, len(expected))),
        CheckResult("16b the at-risk list is exactly the at-risk stations, most urgent first",
                    listed == {s["station_id"] for s in risky} and len(at_risk) == len(risky)
                    and urgency == sorted(urgency),
                    "%d listed, %d flagged in the served states" % (len(at_risk), len(risky))),
        CheckResult("16c served window figures match the gold window row of the same batch", wrong_window == 0,
                    "%d of %d stations differ" % (wrong_window, n)),
        CheckResult("16d only OK stations are flagged at risk", all(s["state"] == "OK" for s in risky),
                    "%d flagged" % len(risky)),
    ]


def availability_history(storage: Storage) -> CheckResult:
    t = read_all(storage, "gold/system_availability_5m",
                 ["bucket_start", "n_stations", "n_operational", "share_empty", "share_full", "share_stale",
                  "share_offline", "n_at_risk_empty", "n_at_risk_full"])
    rows = t.to_pylist()
    buckets = [r["bucket_start"] for r in rows]
    bad = [r for r in rows if not (0 <= r["n_operational"] <= r["n_stations"]
                                   and all(r[k] is None or 0.0 <= r[k] <= 1.0
                                           for k in ("share_empty", "share_full", "share_stale", "share_offline"))
                                   and r["n_at_risk_empty"] + r["n_at_risk_full"] <= r["n_stations"])]
    return CheckResult("17 system availability history: one row per 5-minute bucket, figures in range",
                       len(buckets) == len(set(buckets)) and not bad,
                       "%d buckets, %d duplicates, %d out of range" % (len(buckets), len(buckets) - len(set(buckets)),
                                                                       len(bad)))


def gold(storage: Storage, cfg: PipelineConfig) -> List[CheckResult]:
    serving = serving_follows_gold(storage)
    windows = read_all(storage, "gold/station_window_15m", _WINDOW_COLS)
    return (window_rules(windows, cfg) + [recompute_windows(storage, windows, cfg)] + serving
            + [availability_history(storage)])
