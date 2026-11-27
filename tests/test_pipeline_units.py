import os
from datetime import datetime, timezone

import pytest

from citibike_contract import TOPIC_STATUS, Envelope, encode
from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.decoded_batch import DecodedBatch
from citibike_pipeline.kafka_io import Record
from citibike_pipeline.obs import Obs
from citibike_pipeline.observations import Rejected, StationStatus, WeatherObservation
from citibike_pipeline.station_state import station_state
from citibike_pipeline.storage import LocalStorage, S3Storage
from citibike_pipeline.validation import validate_station_info_payload
from citibike_pipeline.window_metrics import WindowMetrics

CFG = PipelineConfig()
NOW = 1_760_000_000.0


def env(**over):
    """Wire bytes of a valid status envelope, built with the contract; keyword arguments override fields."""
    fields = dict(event_type="station_status", poll_id="p1", source="citibike", source_url="https://gbfs.example",
                  gbfs_version="2.3", feed_last_updated=1_760_968_790,
                  fetched_at=datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc), producer_version="0.1",
                  payload=good_status(last_reported=1_760_968_740))
    fields.update(over)
    return encode(Envelope(**fields))


def test_decode_copies_envelope_provenance_into_the_raw_row():
    rec = Record(topic=TOPIC_STATUS, partition=0, offset=7, timestamp_ms=None, key="a", value=env())
    batch = DecodedBatch.decode([rec], "b1", datetime(2026, 10, 20, 14, 1, tzinfo=timezone.utc), CFG)
    assert batch.dead_letters == ()
    raw = batch.raw_rows[0]
    assert (raw.poll_id, raw.source, raw.source_url, raw.gbfs_version, raw.feed_last_updated, raw.fetched_at,
            raw.producer_version, raw.replay, raw.replayed_at) == (
        "p1", "citibike", "https://gbfs.example", "2.3", 1_760_968_790,
        datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc), "0.1", False, None)
    assert (raw.kafka_topic, raw.kafka_offset, raw.event_type, raw.batch_id) == (
        TOPIC_STATUS, 7, "station_status", "b1")


def good_status(**over):
    p = {"station_id": "a", "num_bikes_available": 3, "num_docks_available": 7, "num_ebikes_available": 1,
         "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": int(NOW) - 60}
    p.update(over)
    return p


@pytest.mark.parametrize("over,reason", [
    ({"station_id": ""}, "bad_station_id"),
    ({"num_bikes_available": -1}, "bad_num_bikes_available"),
    ({"num_docks_available": "7"}, "bad_num_docks_available"),
    ({"num_bikes_available": True}, "bad_num_bikes_available"),
    ({"is_renting": 3}, "bad_is_renting"),
    ({"last_reported": None}, "missing_last_reported"),
    ({"last_reported": int(NOW) + 3600}, "last_reported_in_future"),
])
def test_status_validation_rules(over, reason):
    rejected = StationStatus.parse(good_status(**over), NOW, 300)
    assert isinstance(rejected, Rejected) and reason in rejected.reasons


def test_status_validation_accepts_good_and_small_clock_skew():
    assert isinstance(StationStatus.parse(good_status(), NOW, 300), StationStatus)
    assert isinstance(StationStatus.parse(good_status(last_reported=int(NOW) + 100), NOW, 300), StationStatus)


def test_status_parse_gives_typed_fields_and_defaults_the_flags():
    st = StationStatus.parse({"station_id": "a", "num_bikes_available": 3, "num_docks_available": 7,
                              "last_reported": 5}, NOW, 300)
    assert st == StationStatus(station_id="a", last_reported=5, num_bikes_available=3, num_docks_available=7,
                               num_ebikes_available=None, num_bikes_disabled=None, num_docks_disabled=None,
                               is_installed=True, is_renting=True, is_returning=True)


def test_status_parse_reports_every_reason():
    rejected = StationStatus.parse({"station_id": "", "num_bikes_available": -1}, NOW, 300)
    assert rejected == Rejected(("bad_station_id", "bad_num_bikes_available", "bad_num_docks_available",
                                 "missing_last_reported"))


def test_weather_parse():
    obs = WeatherObservation.parse({"location_id": "nyc", "current": {"time": 1, "temperature_2m": 3.0}})
    assert isinstance(obs, WeatherObservation) and obs.location_id == "nyc" and obs.temperature_c == 3.0
    assert obs.obs_time == datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc) and obs.rain_mm is None
    assert WeatherObservation.parse({}) == Rejected(("missing_current",))
    assert WeatherObservation.parse({"current": {"time": "x", "temperature_2m": "hot"}}) == Rejected(
        ("bad_obs_time", "bad_temperature"))


def test_other_payload_validators():
    assert "bad_coordinates" in validate_station_info_payload({"station_id": "a", "lat": "x", "lon": 1})


def series(bikes, docks=None, step=30.0):
    docks = docks or [30 - b for b in bikes]
    return [Obs(NOW - (len(bikes) - 1 - i) * step, b, d) for i, (b, d) in enumerate(zip(bikes, docks))]


def test_depleting_station_is_at_risk_of_empty():
    # 31 observations 30 s apart; the window is (as_of - 15 min, as_of], so the oldest point is excluded
    # and coverage is 14.5 minutes.
    wm = WindowMetrics.over(series([10] * 15 + [9] * 8 + [7] * 8), NOW, CFG)          # 10 -> 7
    assert wm.net_change == -3 and wm.coverage_min == pytest.approx(14.5)
    assert wm.rate_per_min == pytest.approx(-3 / 14.5)
    assert wm.minutes_to_empty == pytest.approx(7 / (3 / 14.5)) and not wm.at_risk_empty   # ~33.8 > 30
    wm = WindowMetrics.over(series([10] * 10 + [8] * 10 + [4] * 11), NOW, CFG)        # 10 -> 4
    assert wm.minutes_to_empty == pytest.approx(4 / (6 / 14.5)) and wm.at_risk_empty        # ~9.7 min


def test_filling_station_is_at_risk_of_full():
    wm = WindowMetrics.over(series([20] * 10 + [24] * 10 + [27] * 11), NOW, CFG)      # docks 10 -> 3
    assert wm.at_risk_full and wm.minutes_to_full == pytest.approx(3 / (7 / 14.5))


def test_noise_floor_and_flat_series_do_not_alert():
    assert not WindowMetrics.over(series([5] * 5 + [4] * 26), NOW, CFG).at_risk_empty   # only -1 bike
    wm = WindowMetrics.over(series([8] * 31), NOW, CFG)
    assert wm.minutes_to_empty is None and not wm.at_risk_empty and not wm.at_risk_full


def test_short_coverage_gives_no_rate_but_empty_still_reports_zero():
    wm = WindowMetrics.over(series([3, 2, 1], step=30.0), NOW, CFG)        # 1 minute of history
    assert wm.rate_per_min is None and not wm.at_risk_empty
    assert WindowMetrics.over(series([0, 0, 0], step=30.0), NOW, CFG).minutes_to_empty == 0.0
    assert WindowMetrics.over([], NOW, CFG) is None


def test_observations_outside_window_are_ignored():
    obs = series([5] * 3) + [Obs(NOW - 3600, 30, 0)]
    assert WindowMetrics.over(obs, NOW, CFG).n_obs == 3


def test_window_at_risk_names_empty_before_full():
    assert WindowMetrics.over(series([10] * 10 + [8] * 10 + [4] * 11), NOW, CFG).at_risk == "EMPTY"
    assert WindowMetrics.over(series([20] * 10 + [24] * 10 + [27] * 11), NOW, CFG).at_risk == "FULL"
    assert WindowMetrics.over(series([8] * 31), NOW, CFG).at_risk is None


def test_station_state_precedence():
    assert station_state(True, True, True, False, 0, 5) == "EMPTY"
    assert station_state(True, True, True, False, 5, 0) == "FULL"
    assert station_state(True, True, True, True, 0, 5) == "STALE"
    assert station_state(True, False, True, True, 0, 5) == "OFFLINE"
    assert station_state(True, True, True, False, 5, 5) == "OK"


def test_local_storage_roundtrip_and_safety(tmp_path):
    s = LocalStorage(str(tmp_path))
    s.put("a/b/c.bin", b"x")
    assert s.get("a/b/c.bin") == b"x" and s.exists("a/b/c.bin") and s.list("a/") == ["a/b/c.bin"]
    s.put("a/b/c.bin", b"y")                                  # overwrite is atomic and idempotent
    assert s.get("a/b/c.bin") == b"y"
    s.delete("a/b/c.bin")
    s.delete("a/b/c.bin")                                     # deleting twice is fine
    assert not s.exists("a/b/c.bin")
    with pytest.raises(ValueError):
        s.put("../escape", b"x")
    with pytest.raises(FileNotFoundError):
        s.get("missing")


LIST_KEYS = ["raw/a/1.p", "raw/ab/2.p", "raw/b.p", "rawx/3.p", "top.p"]
LIST_PREFIXES = ["", "raw/", "raw/a", "raw/a/", "raw/ab/", "ra", "nope/", "raw/a/1.p"]


def assert_list_is_plain_prefix_match(s):
    """list(prefix) means every key starting with prefix, on every backend (S3 semantics)."""
    for k in LIST_KEYS:
        s.put(k, b"x")
    for p in LIST_PREFIXES:
        assert s.list(p) == sorted(k for k in LIST_KEYS if k.startswith(p)), p


def test_local_storage_list_matches_s3_prefix_semantics(tmp_path):
    assert_list_is_plain_prefix_match(LocalStorage(str(tmp_path)))


def test_local_storage_list_does_not_walk_outside_the_prefix(tmp_path, monkeypatch):
    s = LocalStorage(str(tmp_path))
    s.put("curated/x/dt=2026-10-01/hr=00/a.parquet", b"x")
    s.put("ops/batch_plan/r1.json", b"x")
    walked = []
    real_walk = os.walk
    monkeypatch.setattr(os, "walk", lambda top: walked.append(top) or real_walk(top))
    assert s.list("curated/x/dt=2026-10-01/hr=00/") == ["curated/x/dt=2026-10-01/hr=00/a.parquet"]
    assert walked == [os.path.join(str(tmp_path), "curated", "x", "dt=2026-10-01", "hr=00")]
    with pytest.raises(ValueError):
        s.list("../outside/")


def test_s3_storage_with_moto(monkeypatch):
    moto = pytest.importorskip("moto")
    boto3 = pytest.importorskip("boto3")
    mock = getattr(moto, "mock_aws", None) or moto.mock_s3
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "t")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "t")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="lake")
        s = S3Storage("lake", "citibike", client=client)
        s.put("raw/x.bin", b"1")
        s.put("raw/y.bin", b"2")
        assert s.exists("raw/x.bin") and not s.exists("raw/z.bin")
        assert s.list("raw/") == ["raw/x.bin", "raw/y.bin"] and s.get("raw/y.bin") == b"2"
        s.delete("raw/x.bin")
        assert s.list("raw/") == ["raw/y.bin"]
        with pytest.raises(FileNotFoundError):
            s.get("raw/x.bin")
        assert_list_is_plain_prefix_match(S3Storage("lake", "other", client=client))
