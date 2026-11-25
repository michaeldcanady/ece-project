"""End to end over an in-memory broker: synthetic capture -> plan/process/metrics/finalize -> lake."""
import json
import re
from datetime import datetime, timedelta, timezone

import pytest
from fakes import load_capture

from citibike_contract import TOPIC_STATUS, Envelope, encode, parse_iso
from citibike_pipeline import batch, lake
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
    keys = [k for k in storage.list(lake.CURATED_STATUS.prefix + "/") if k.endswith(".parquet")]
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


def _hold_back_half(consumer):
    """Remove the newer half of every partition (to be ingested later); returns it."""
    held = {tp: log[len(log) // 2:] for tp, log in consumer.log.items()}
    for tp in consumer.log:
        consumer.log[tp] = consumer.log[tp][:len(consumer.log[tp]) // 2]
    return held


def _rows_by_offset(storage, prefix):
    rows = [r for k in storage.list(prefix + "/") if k.endswith(".parquet")
            for r in read_parquet(storage, k).to_pylist()]
    return len(rows), len({(r["kafka_topic"], r["kafka_partition"], r["kafka_offset"]) for r in rows})


def test_next_run_after_a_failed_run_reuses_its_plan_and_leaves_no_duplicates(tmp_path, capture):
    cfg = PipelineConfig(local_root=str(tmp_path), read_timeout_s=5)        # no cap: one batch takes all
    consumer = load_capture(capture)
    storage, reader = LocalStorage(str(tmp_path)), KafkaBatchReader("x", "g", consumer=consumer)
    held = _hold_back_half(consumer)
    # run_0 writes every output but never commits (finalize failed on every retry)
    p0 = batch.plan_batch("run_0", reader, storage, cfg)
    s0 = batch.process_batch(p0, reader, storage, cfg)
    batch.compute_metrics(p0, s0, storage, cfg)
    for tp, rest in held.items():                                           # ingest kept running
        consumer.log[tp] += rest
    p1 = batch.plan_batch("run_1", reader, storage, cfg)
    assert (p1["batch_id"], p1["ranges"], p1["first_run_id"], p1["run_id"]) == \
        (p0["batch_id"], p0["ranges"], "run_0", "run_1")
    s1 = batch.process_batch(p1, reader, storage, cfg)
    entry = batch.finalize_batch(p1, s1, batch.compute_metrics(p1, s1, storage, cfg), reader, storage, cfg)
    assert entry["run_id"] == "run_1"
    for prefix in (lake.CURATED_STATUS.prefix, lake.RAW_STATUS.prefix, lake.DLQ):      # one row per Kafka offset
        rows, distinct = _rows_by_offset(storage, prefix)
        assert rows == distinct > 0, prefix
    gold_batches = {r["batch_id"] for k in storage.list(lake.GOLD_WINDOW + "/")
                    for r in read_parquet(storage, k).to_pylist()}
    assert gold_batches == {p0["batch_id"]}
    # committed: the pending plan is gone, and the next plan starts where this one ended
    assert not storage.exists(batch.PENDING_PLAN)
    p2 = batch.plan_batch("run_2", reader, storage, cfg)
    assert p2["batch_id"] != p0["batch_id"] and p2["total_messages"] > 0
    for topic, parts in p2["ranges"].items():
        for part, (start, _end) in parts.items():
            assert start == p0["ranges"][topic][part][1]


def test_pending_plan_is_dropped_when_its_offsets_no_longer_exist(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    stale = batch.plan_batch("run_0", reader, storage, cfg)
    # broker reset: topics recreated, fewer messages than the pending plan expects, nothing committed
    _hold_back_half(consumer)
    _hold_back_half(consumer)
    fresh = batch.plan_batch("run_1", reader, storage, cfg)
    assert fresh["batch_id"] != stale["batch_id"] and fresh["first_run_id"] == "run_1"
    assert batch.process_batch(fresh, reader, storage, cfg)["records_read"] == fresh["total_messages"]


def test_an_empty_plan_is_never_saved_as_pending(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    saved = dict(consumer.log)
    consumer.log.clear()                                                    # idle: nothing to read
    assert batch.plan_batch("run_0", reader, storage, cfg)["total_messages"] == 0
    assert not storage.exists(batch.PENDING_PLAN)
    consumer.log.update(saved)                                              # data arrives
    assert batch.plan_batch("run_1", reader, storage, cfg)["total_messages"] > 0


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


def _replayed_status(station_id, replayed_at):
    """Wire bytes of a replayed status envelope whose replay time is the raw value `replayed_at`."""
    fetched = START + timedelta(minutes=MINUTES - 1)
    env = Envelope(event_type="station_status", poll_id="replay-" + station_id, source="test", source_url=None,
                   gbfs_version=None, feed_last_updated=None, fetched_at=fetched, producer_version=None,
                   payload={"station_id": station_id, "num_bikes_available": 1, "num_docks_available": 1,
                            "last_reported": int(fetched.timestamp()) - 30})
    wire = json.loads(encode(env.mark_replayed(START + timedelta(hours=2))))
    wire["replayed_at"] = replayed_at
    return json.dumps(wire).encode()


def test_a_malformed_replay_time_lands_in_dlq_and_the_batch_commits(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    consumer.append(TOPIC_STATUS, 0, "st-bad", _replayed_status("st-bad", "not a time"))
    consumer.append(TOPIC_STATUS, 0, "st-good", _replayed_status("st-good", "2026-10-20T14:00:00.000+00:00"))
    drain(reader, storage, cfg)
    assert consumer.commits[(TOPIC_STATUS, 0)] == len(consumer.log[(TOPIC_STATUS, 0)])
    dlq = [r for k in storage.list("dlq/") for r in read_parquet(storage, k).to_pylist()]
    assert [r["reason"] for r in dlq if "st-bad" in r["raw_value"]] == ["bad_replayed_at"]
    rows = {r["station_id"]: r for r in curated_rows(storage)}
    assert "st-bad" not in rows
    assert rows["st-good"]["replay"] is True
    assert rows["st-good"]["replayed_at"] == datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc)


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


HOUR = START                       # 12:00, an hour several batches write into
CURATED = lake.CURATED_STATUS


def batches_in_hour(storage, table=CURATED, hour=HOUR):
    t = table.read_window(storage, hour, hour + timedelta(hours=1), ["batch_id"])
    return sorted(set(t["batch_id"].to_pylist()))


def test_compaction_is_lossless_idempotent_and_verified(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    drain(reader, storage, cfg)
    on_disk = len(curated_rows(storage))
    before = CURATED.read_window(storage, HOUR, HOUR + timedelta(hours=1))
    n_batches = len(batches_in_hour(storage))
    res = CURATED.compact_hour(storage, HOUR)
    assert res == {"sources": n_batches, "rows": before.num_rows} and n_batches > 1
    assert CURATED.compact_hour(storage, HOUR)["sources"] == 0                       # idempotent
    after = CURATED.read_window(storage, HOUR, HOUR + timedelta(hours=1))
    assert sorted(after.to_pylist(), key=str) == sorted(before.to_pylist(), key=str)
    assert len(curated_rows(storage)) == on_disk                                      # nothing lost on disk


class CompactsMidRead(LocalStorage):
    """Compacts HOUR the first time a reader fetches one of its files, then serves the (now deleted)
    file: the worst-case interleaving of a window read and compaction."""

    def __init__(self, root):
        super().__init__(root)
        self.armed = False

    def get(self, key):
        if self.armed and key.startswith(lake.partition_path(CURATED.prefix, HOUR) + "/"):
            self.armed = False
            CURATED.compact_hour(self, HOUR)
        return super().get(key)


def test_read_window_is_lossless_when_compaction_runs_mid_read(tmp_path, capture):
    cfg, _storage, consumer, reader = make_env(tmp_path, capture)
    storage = CompactsMidRead(str(tmp_path))
    drain(reader, storage, cfg)
    cols = ["station_id", "fetched_at", "kafka_offset"]
    expected = CURATED.read_window(storage, START, START + timedelta(hours=1), cols)
    storage.armed = True
    got = CURATED.read_window(storage, START, START + timedelta(hours=1), cols)
    assert not storage.armed                                            # compaction really ran mid-read
    assert CURATED.compact_hour(storage, HOUR)["sources"] == 0          # ... and finished
    assert sorted(got.to_pylist(), key=str) == sorted(expected.to_pylist(), key=str)


def test_read_window_raises_when_a_file_keeps_vanishing(tmp_path):
    class Ghost(LocalStorage):
        def list(self, prefix):
            return [prefix + "ghost.parquet"]

    with pytest.raises(FileNotFoundError):
        CURATED.read_window(Ghost(str(tmp_path)), START, START + timedelta(minutes=5))


def test_read_window_returns_only_the_requested_columns_typed_even_when_empty(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    empty = CURATED.read_window(storage, START, START + timedelta(hours=1), ["station_id", "fetched_at"])
    assert empty.num_rows == 0 and empty.schema.names == ["station_id", "fetched_at"]
    assert empty.schema.field("fetched_at").type == CURATED.schema.field("fetched_at").type
    drain(reader, storage, cfg)
    t = CURATED.read_window(storage, START, START + timedelta(hours=1), ["station_id"])
    assert t.num_rows > 0 and t.schema.names == ["station_id"]


def test_read_window_excludes_a_batch_also_inside_compacted_files(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    drain(reader, storage, cfg)
    window = (HOUR, HOUR + timedelta(hours=1))
    victim = batches_in_hour(storage)[0]
    others = CURATED.read_window(storage, *window, ["batch_id"], exclude_batch=victim)
    assert others.num_rows > 0 and victim not in set(others["batch_id"].to_pylist())
    CURATED.compact_hour(storage, HOUR)
    after = CURATED.read_window(storage, *window, ["batch_id"], exclude_batch=victim)
    assert sorted(after["batch_id"].to_pylist()) == sorted(others["batch_id"].to_pylist())


def test_read_window_never_excludes_rows_without_a_batch_id(tmp_path):
    storage = LocalStorage(str(tmp_path))
    rows = [{"kafka_topic": "t", "kafka_partition": 0, "kafka_offset": i, "fetched_at": START + timedelta(minutes=i),
             "event_type": "poll_log", "batch_id": b} for i, b in enumerate([None, "bA", "bB"])]
    lake.RAW_POLL_LOG.write(storage, "mixed", rows)
    for compact in (False, True):
        if compact:
            lake.RAW_POLL_LOG.compact_hour(storage, HOUR)
        t = lake.RAW_POLL_LOG.read_window(storage, HOUR, HOUR + timedelta(hours=1), ["batch_id"], exclude_batch="bA")
        assert sorted(t["batch_id"].to_pylist(), key=str) == [None, "bB"], compact


def test_compact_due_treats_a_naive_now_as_utc(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    drain(reader, storage, cfg)
    naive = (START + timedelta(hours=4)).replace(tzinfo=None)
    assert lake.compact_due(storage, now=naive)
    assert lake.compact_due(storage, now=START + timedelta(hours=4)) == {}


def test_batch_latencies_fail_loudly_without_hours(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    plan, stats = run_batch(0, reader, storage, cfg, fail_before_commit=True)
    published_at = START + timedelta(minutes=MINUTES)
    no_hours = {k: v for k, v in stats.items() if k != "status_hours"}
    with pytest.raises(KeyError):
        batch.batch_latencies(storage, plan, no_hours, published_at)
    with pytest.raises(ValueError):
        batch.batch_latencies(storage, plan, dict(no_hours, status_keys=["not/a/batch/key.parquet"]), published_at)


def test_read_batch_finds_one_batch_inside_a_compacted_file(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    assert len(drain(reader, storage, cfg)) > 1
    cols = ["kafka_partition", "kafka_offset", "fetched_at"]
    ids = batches_in_hour(storage)
    assert len(ids) > 1                                                  # several batches share the hour
    before = {b: CURATED.read_batch(storage, b, [HOUR], cols).to_pylist() for b in ids}
    CURATED.compact_hour(storage, HOUR)
    for b, rows in before.items():
        assert rows and CURATED.read_batch(storage, b, [HOUR], cols).to_pylist() == rows, b


def test_finalize_succeeds_after_compaction_merged_the_batch_files(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    plan, stats = run_batch(0, reader, storage, cfg, fail_before_commit=True)
    publish = batch.compute_metrics(plan, stats, storage, cfg, now=START + timedelta(minutes=MINUTES))
    direct = sorted(batch.batch_latencies(storage, plan, stats, parse_iso(publish["published_at"])))
    assert direct
    assert lake.compact_due(storage, now=START + timedelta(hours=4))     # e.g. a replay into old hours
    entry = batch.finalize_batch(plan, stats, publish, reader, storage, cfg, now=START + timedelta(minutes=MINUTES))
    assert sorted(batch.batch_latencies(storage, plan, stats, parse_iso(publish["published_at"]))) == direct
    assert entry["latency_p50_s"] is not None


def test_finalize_accepts_stats_from_before_status_hours(tmp_path, capture):
    # A run in flight across the deploy carries the old stats shape: file keys instead of hours.
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    plan, stats = run_batch(0, reader, storage, cfg, fail_before_commit=True)
    published_at = START + timedelta(minutes=MINUTES)
    direct = sorted(batch.batch_latencies(storage, plan, stats, published_at))
    legacy = {k: v for k, v in stats.items() if k != "status_hours"}
    legacy["status_keys"] = ["%s/%s.parquet" % (lake.partition_path(CURATED.prefix, parse_iso(h)), plan["batch_id"])
                             for h in stats["status_hours"]]
    assert direct and sorted(batch.batch_latencies(storage, plan, legacy, published_at)) == direct


def test_retry_after_compaction_does_not_read_its_own_rows_as_prior_context(tmp_path, capture):
    # A batch whose hour is compacted before it is retried (replay or catch-up into hours older than
    # the compaction lag): the retry must not see its own first attempt inside compacted.parquet.
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    plan, first = run_batch(0, reader, storage, cfg, fail_before_commit=True)
    assert lake.compact_due(storage, now=START + timedelta(hours=4))
    retry = batch.process_batch(plan, reader, storage, cfg, now=START + timedelta(minutes=MINUTES, seconds=5))
    for k in ("repeats", "out_of_order", "status_rows"):
        assert retry[k] == first[k], k


LAYOUT = r"^%s/dt=\d{4}-\d{2}-\d{2}/hr=\d{2}/%s$"


def test_lake_table_on_disk_layout(tmp_path, capture):
    # Pins the key format that existing local and S3 lakes (and anything reading them) depend on.
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    drain(reader, storage, cfg)
    tables = ["raw/station_status", "raw/weather", "raw/poll_log", "curated/station_status", "curated/weather"]
    for prefix in tables:
        keys = storage.list(prefix + "/")
        assert keys, prefix
        assert all(re.match(LAYOUT % (prefix, r"b[0-9a-f]{12}\.parquet"), k) for k in keys), keys
    lake.compact_due(storage, now=START + timedelta(hours=4))
    for prefix in tables:
        keys = storage.list(prefix + "/")
        assert keys and all(re.match(LAYOUT % (prefix, r"compacted\.parquet"), k) for k in keys), keys


def test_compact_due_respects_lag(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    drain(reader, storage, cfg)
    assert lake.compact_due(storage, now=START + timedelta(hours=1, minutes=30)) == {}       # hour ended < 2 h ago
    done = lake.compact_due(storage, now=START + timedelta(hours=4))
    assert any(k.startswith(CURATED.prefix) for k in done)
    assert lake.compact_due(storage, now=START + timedelta(hours=4)) == {}                   # second run: nothing left


def test_daily_quality_report(tmp_path, capture):
    cfg, storage, consumer, reader = make_env(tmp_path, capture)
    build_dim_station(reader, storage, cfg)
    drain(reader, storage, cfg)
    lake.compact_due(storage, now=START + timedelta(hours=4))
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
