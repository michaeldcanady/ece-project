"""End to end over an in-memory broker: synthetic capture -> plan/process/metrics/finalize -> lake."""
from datetime import datetime, timedelta, timezone

import pytest
from fakes import load_capture

from citibike_contract import parse_iso
from citibike_pipeline import batch, lake
from citibike_pipeline.compaction import compact_due, compact_partition
from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.kafka_io import KafkaBatchReader
from citibike_pipeline.quality import daily_quality
from citibike_pipeline.station_info import build_dim_station
from citibike_pipeline.storage import LocalStorage, read_parquet
from citibike_producer.synthetic import generate

START = datetime(2026, 10, 20, 12, 0, 0, tzinfo=timezone.utc)
MINUTES = 40


@pytest.fixture(scope="module")
def capture(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("cap") / "cap.jsonl.gz")
    generate(path, stations=40, minutes=MINUTES, start=START)
    return path


def make_env(tmp_path, capture, cap=700):
    cfg = PipelineConfig(local_root=str(tmp_path), max_batch_per_partition=cap, read_timeout_s=5)
    consumer = load_capture(capture)
    return cfg, LocalStorage(str(tmp_path)), consumer, KafkaBatchReader("x", "g", consumer=consumer)


def run_batch(i, reader, storage, cfg, fail_before_commit=False):
    now = START + timedelta(minutes=MINUTES, seconds=5 + i)
    plan = batch.plan_batch("run_%d" % i, reader, storage, cfg, now=now)
    if plan["total_messages"] == 0:
        return plan, None
    stats = batch.process_batch(plan, reader, storage, cfg, now=now)
    publish = batch.compute_metrics(plan, stats, storage, cfg, now=now)
    if fail_before_commit:
        return plan, stats
    return plan, batch.finalize_batch(plan, stats, publish, reader, storage, cfg, now=now)


def drain(reader, storage, cfg, max_batches=50):
    n, plans = 0, []
    while n < max_batches:
        plan, entry = run_batch(n, reader, storage, cfg)
        if entry is None:
            break
        plans.append((plan, entry))
        n += 1
    return plans


def curated_rows(storage):
    keys = [k for k in storage.list(lake.CURATED_STATUS + "/") if k.endswith(".parquet")]
    rows = []
    for k in keys:
        rows.extend(read_parquet(storage, k).to_pylist())
    return rows


def test_plan_is_stable_across_retries_and_advances_after_commit(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    p1 = batch.plan_batch("run_0", reader, storage, cfg)
    assert p1["total_messages"] > 0 and p1["batch_id"].startswith("b")
    # retry with the same run id: identical plan even though nothing was committed
    assert batch.plan_batch("run_0", reader, storage, cfg) == p1
    # a new run before any commit plans the same ranges (nothing was consumed yet)
    assert batch.plan_batch("run_1", reader, storage, cfg)["ranges"] == p1["ranges"]


def test_full_drain_commits_everything_and_loses_nothing(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    plans = drain(reader, storage, cfg)
    assert len(plans) > 1, "cap should force several batches"
    # every partition committed up to its high watermark
    for (topic, part), entries in consumer.log.items():
        if topic != "citibike.station_info":
            assert consumer.commits[(topic, part)] == len(entries)
    total_read = sum(e["records_read"] for _p, e in plans)
    assert total_read == sum(len(v) for (t, _p), v in consumer.log.items() if t != "citibike.station_info")
    # accounting: raw rows + DLQ-without-raw == read; nothing silently vanishes
    for _p, e in plans:
        assert e["status_rows"] + e["duplicates_dropped"] <= e["raw_rows"]


def test_validation_flags_land_in_dlq_not_curated(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    drain(reader, storage, cfg)
    dlq = [r for k in storage.list("dlq/") for r in read_parquet(storage, k).to_pylist()]
    assert len(dlq) == MINUTES                                     # st-005 has -1 bikes in every poll
    assert {r["reason"] for r in dlq} == {"bad_num_bikes_available"}
    assert all("Synthetic" not in r["raw_value"] for r in dlq) and all(r["raw_value"] for r in dlq)
    rows = curated_rows(storage)
    assert "st-005" not in {r["station_id"] for r in rows}
    assert all(r["num_bikes_available"] >= 0 for r in rows)


def test_curated_flags_and_provenance(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    build_dim_station(reader, storage, cfg)
    drain(reader, storage, cfg)
    rows = curated_rows(storage)
    ids = {(r["kafka_topic"], r["kafka_partition"], r["kafka_offset"]) for r in rows}
    assert len(ids) == len(rows), "no duplicated Kafka coordinates in curated"
    by = {}
    for r in rows:
        by.setdefault(r["station_id"], []).append(r)
    assert all(r["is_stale"] for r in by["st-003"])                    # last_reported 3 h old
    assert not any(r["is_stale"] for r in by["st-010"])
    assert all(r["is_unknown_station"] for r in by["st-unknown"])      # not in the dimension
    assert not any(r["is_unknown_station"] for r in by["st-010"])
    assert by["st-010"][0]["station_name"] == "Synthetic St 10" and by["st-010"][0]["capacity"] > 0
    assert all(r["temperature_c"] is not None for r in by["st-010"])   # as-of weather join
    assert any(r["is_repeat"] for r in by["st-003"][1:])               # unchanged last_reported
    assert not any(r["is_out_of_order"] for r in rows)
    first = min(r["fetched_at"] for r in rows)
    assert not by["st-010"][0]["is_repeat"] and by["st-010"][0]["fetched_at"] >= first


def test_serving_files_reflect_scenario(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    build_dim_station(reader, storage, cfg)
    drain(reader, storage, cfg)
    state = {r["station_id"]: r for r in read_parquet(storage, lake.SERVING_CURRENT).to_pylist()}
    assert state["st-002"]["state"] == "EMPTY"
    assert state["st-003"]["state"] == "STALE"
    assert state["st-004"]["state"] == "OFFLINE"
    assert state["st-000"]["at_risk"] in ("EMPTY", None)               # depleting 1 bike / 2 min from 18
    at_risk = {r["station_id"]: r for r in read_parquet(storage, lake.SERVING_AT_RISK).to_pylist()}
    assert state["st-000"]["net_change_15m"] < 0
    assert state["st-001"]["net_change_15m"] > 0
    assert state["st-001"]["at_risk"] == "FULL" and state["st-001"]["minutes_to_full"] < cfg.risk_horizon_minutes
    assert "st-002" not in at_risk and "st-003" not in at_risk and "st-004" not in at_risk
    summary = read_parquet(storage, lake.SERVING_SUMMARY).to_pylist()[0]
    assert summary["n_stations"] == 40 + 1 - 1                          # 40 real + unknown, minus invalid st-005
    assert 0 < summary["share_empty"] < 1 and summary["share_offline"] > 0


def test_depleting_station_becomes_at_risk_before_it_empties(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture, cap=120)      # ~3 minutes of data per batch
    drain(reader, storage, cfg, max_batches=200)
    windows = [r for k in storage.list(lake.GOLD_WINDOW + "/") for r in read_parquet(storage, k).to_pylist()
               if r["station_id"] == "st-000"]
    flagged = [w for w in windows if w["at_risk_empty"]]
    assert flagged, "st-000 loses a bike every 2 min from 18 and must be flagged before reaching zero"
    assert all(w["bikes_last"] > 0 for w in flagged)
    assert all(w["minutes_to_empty"] <= cfg.risk_horizon_minutes for w in flagged)


def test_retry_after_crash_before_commit_is_idempotent(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    plan, _ = run_batch(0, reader, storage, cfg, fail_before_commit=True)   # outputs written, offsets NOT committed
    keys_after_crash = storage.list("")
    assert consumer.commits == {}
    # Airflow retries the same run: same plan, same keys, same rows
    plan2, entry = run_batch(0, reader, storage, cfg)
    assert plan2 == plan and entry is not None
    new_keys = set(storage.list("")) - set(keys_after_crash)
    assert all(k.startswith(("ops/batch_log", "gold/serving")) or k in keys_after_crash for k in new_keys)
    rows = curated_rows(storage)
    ids = [(r["kafka_topic"], r["kafka_partition"], r["kafka_offset"]) for r in rows]
    assert len(ids) == len(set(ids)) and consumer.commits           # no duplicates; committed now


def test_next_batch_does_not_reread_committed_offsets(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    p0, e0 = run_batch(0, reader, storage, cfg)
    p1, e1 = run_batch(1, reader, storage, cfg)
    for topic, parts in p1["ranges"].items():
        for part, (s, _e) in parts.items():
            assert s == p0["ranges"][topic][part][1]


def test_batch_log_holds_measured_values_not_constants(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    plans = drain(reader, storage, cfg)
    entries = [e for _p, e in plans if e["status_rows"]]
    p50s = {e["latency_p50_s"] for e in entries}
    assert len(p50s) > 1 and all(e["latency_p50_s"] >= 0 for e in entries)
    assert all(e["latency_p95_s"] >= e["latency_p50_s"] for e in entries)
    recent = read_parquet(storage, lake.SERVING_BATCH_LOG).to_pylist()
    assert len({r["batch_id"] for r in recent}) == len(recent) == len(plans)


def test_station_info_builds_dimension_and_refuses_empty(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    out = build_dim_station(reader, storage, cfg)
    assert out["stations"] == 40
    dim = read_parquet(storage, lake.DIM_STATION_CURRENT).to_pylist()
    assert len(dim) == 40 and {"lat", "lon", "capacity"} <= set(dim[0])
    consumer.log[("citibike.station_info", 0)] = []
    with pytest.raises(RuntimeError):
        build_dim_station(reader, storage, cfg)
    assert len(read_parquet(storage, lake.DIM_STATION_CURRENT).to_pylist()) == 40   # not overwritten


def test_compaction_is_lossless_idempotent_and_verified(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    drain(reader, storage, cfg)
    before = curated_rows(storage)
    prefix, dt, hr = lake.CURATED_STATUS, "2026-10-20", 12
    n_files = len([k for k in storage.list("%s/dt=%s/hr=%d/" % (prefix, dt, hr))])
    res = compact_partition(storage, prefix, dt, hr, ["fetched_at", "kafka_partition", "kafka_offset"])
    assert res["sources"] == n_files > 1
    keys = storage.list("%s/dt=%s/hr=%d/" % (prefix, dt, hr))
    assert keys == ["%s/dt=%s/hr=%d/compacted.parquet" % (prefix, dt, hr)]
    after = curated_rows(storage)
    assert len(after) == len(before)
    assert compact_partition(storage, prefix, dt, hr, ["fetched_at"])["sources"] == 0      # idempotent
    # window reads see the compacted file exactly once
    t = lake.load_window(storage, prefix, START, START + timedelta(hours=1),
                         lake.IDENTITY + ["fetched_at"])
    assert t.num_rows == len(before)


SORT = ["fetched_at", "kafka_partition", "kafka_offset"]
HOUR = (lake.CURATED_STATUS, "2026-10-20", 12)


class CompactsMidRead(LocalStorage):
    """Compacts HOUR the first time a reader fetches one of its minute files, then serves the
    (now deleted) file: the worst-case interleaving of load_window and compaction."""

    def __init__(self, root):
        super().__init__(root)
        self.armed = False

    def get(self, key):
        if self.armed and key.startswith("%s/dt=%s/hr=%d/" % HOUR) and not key.endswith(lake.COMPACTED_NAME):
            self.armed = False
            compact_partition(self, *HOUR, SORT)
        return super().get(key)


def test_load_window_is_lossless_when_compaction_runs_mid_read(tmp_path, capture):
    cfg, _storage, consumer, reader = make_env(tmp_path, capture)
    storage = CompactsMidRead(str(tmp_path))
    drain(reader, storage, cfg)
    cols = lake.IDENTITY + ["fetched_at"]
    expected = lake.load_window(storage, lake.CURATED_STATUS, START, START + timedelta(hours=1), cols)
    storage.armed = True
    got = lake.load_window(storage, lake.CURATED_STATUS, START, START + timedelta(hours=1), cols)
    assert not storage.armed                                            # compaction really ran mid-read
    assert storage.list("%s/dt=%s/hr=%d/" % HOUR) == ["%s/dt=%s/hr=%d/compacted.parquet" % HOUR]
    assert sorted(got.to_pylist(), key=str) == sorted(expected.to_pylist(), key=str)


def test_load_window_raises_when_a_file_keeps_vanishing(tmp_path):
    class Ghost(LocalStorage):
        def list(self, prefix):
            return [prefix + "ghost.parquet"]

    with pytest.raises(FileNotFoundError):
        lake.load_window(Ghost(str(tmp_path)), lake.CURATED_STATUS, START, START + timedelta(minutes=5),
                         lake.IDENTITY + ["fetched_at"])


def test_read_batch_file_finds_one_batch_inside_a_compacted_file(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    assert len(drain(reader, storage, cfg)) > 1                          # several batches share the hour
    cols = ["kafka_partition", "kafka_offset", "fetched_at"]
    before = {k: read_parquet(storage, k, columns=cols).to_pylist()
              for k in storage.list("%s/dt=%s/hr=%d/" % HOUR)}
    compact_partition(storage, *HOUR, SORT)
    for key, rows in before.items():
        batch_id = key.rsplit("/", 1)[1][:-len(".parquet")]
        assert lake.read_batch_file(storage, key, batch_id, cols).to_pylist() == rows, key


def test_finalize_succeeds_after_compaction_merged_the_batch_files(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    plan, stats = run_batch(0, reader, storage, cfg, fail_before_commit=True)
    publish = batch.compute_metrics(plan, stats, storage, cfg, now=START + timedelta(minutes=MINUTES))
    direct = sorted(batch.batch_latencies(storage, plan, stats, parse_iso(publish["published_at"])))
    compact_due(storage, now=START + timedelta(hours=4))                # e.g. a replay into old hours
    assert not any(storage.exists(k) for k in stats["status_keys"])
    entry = batch.finalize_batch(plan, stats, publish, reader, storage, cfg, now=START + timedelta(minutes=MINUTES))
    assert sorted(batch.batch_latencies(storage, plan, stats, parse_iso(publish["published_at"]))) == direct
    assert entry["latency_p50_s"] is not None


def test_compact_due_respects_lag(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    drain(reader, storage, cfg)
    assert compact_due(storage, now=START + timedelta(hours=1, minutes=30)) == {}       # hour ended < 2 h ago
    done = compact_due(storage, now=START + timedelta(hours=4))
    assert any(k.startswith(lake.CURATED_STATUS) for k in done)
    assert compact_due(storage, now=START + timedelta(hours=4)) == {}                   # second run: nothing left


def test_daily_quality_report(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    build_dim_station(reader, storage, cfg)
    drain(reader, storage, cfg)
    compact_due(storage, now=START + timedelta(hours=4))
    rep = daily_quality(storage, cfg, START.date())
    poll = rep["polls"]["station_status"]
    assert poll["polls_logged"] == MINUTES and poll["gaps_over_threshold"] == 0
    assert poll["completeness"] == pytest.approx(MINUTES / 1440.0, abs=1e-4)
    assert rep["verdict"] == "fail"                                    # 40 of 1440 minutes captured
    obs = rep["station_observations"]
    assert obs["distinct_stations"] == 40 and obs["stale_share"] > 0
    assert rep["dlq"]["by_reason"] == {"bad_num_bikes_available": MINUTES}
    assert rep["batches"]["batches"] > 1 and rep["batches"]["latency_s"]["median_of_batch_p50"] is not None
    assert storage.exists("quality/daily/dt=2026-10-20/report.json")
    assert storage.exists("quality/daily/dt=2026-10-20/report.md")
    assert obs["top_empty_stations"][0]["station_id"] == "st-002"


def test_quality_detects_gap(tmp_path):
    cfg = PipelineConfig(local_root=str(tmp_path / "lake"), max_batch_per_partition=100000, read_timeout_s=5)
    path = str(tmp_path / "gap.jsonl.gz")
    generate(path, stations=10, minutes=10, start=START)
    import gzip
    import json
    gap_from, gap_to = START + timedelta(minutes=3), START + timedelta(minutes=6)
    keep = []
    with gzip.open(path, "rt") as fh:
        for line in fh:
            e = json.loads(line)
            if e["event_type"] in ("station_status", "poll_log") and gap_from < parse_iso(e["fetched_at"]) < gap_to:
                continue
            keep.append(line)
    gappy = str(tmp_path / "gappy.jsonl.gz")
    with gzip.open(gappy, "wt") as fh:
        fh.writelines(keep)
    storage = LocalStorage(str(tmp_path / "lake"))
    consumer = load_capture(gappy)
    reader = KafkaBatchReader("x", "g", consumer=consumer)
    drain(reader, storage, cfg)
    rep = daily_quality(storage, cfg, START.date())
    assert rep["polls"]["station_status"]["gaps_over_threshold"] == 1
    assert rep["polls"]["station_status"]["gap_seconds_total"] >= 170


def test_serving_time_never_moves_backwards_when_partitions_skew(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture, cap=150)
    seen = []
    for i in range(200):
        plan, entry = run_batch(i, reader, storage, cfg)
        if entry is None:
            break
        seen.append(read_parquet(storage, lake.SERVING_SUMMARY).to_pylist()[0]["as_of"])
    assert len(seen) > 5 and seen == sorted(seen)


def test_status_reports_fresh_and_stale_lake(tmp_path, capture):
    from citibike_pipeline.status import summarize
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    assert summarize(storage, 5)["ok"] is False                       # nothing published yet
    drain(reader, storage, cfg)
    res = summarize(storage, max_age_min=10 ** 9)
    assert res["ok"] and res["stations"] == 40 and res["batches_last_24h"] > 1
    assert summarize(storage, max_age_min=1)["ok"] is False           # batches are from 2026-10-20: stale now
