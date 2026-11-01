import json
import os

import pytest

from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.metrics import Obs, station_state, window_metrics
from citibike_pipeline.storage import LocalStorage, S3Storage
from citibike_pipeline.validation import (
    parse_envelope,
    validate_poll_log_payload,
    validate_station_info_payload,
    validate_status_payload,
    validate_weather_payload,
)

CFG = PipelineConfig()
NOW = 1_760_000_000.0


def env(**over):
    base = {"schema_version": 1, "event_type": "station_status", "poll_id": "p1",
            "fetched_at": "2026-10-20T14:00:00.000+00:00", "payload": {"station_id": "1"}}
    base.update(over)
    return json.dumps(base).encode()


def test_parse_envelope_accepts_and_rejects():
    e, errs = parse_envelope(env())
    assert errs == [] and e["poll_id"] == "p1"
    assert parse_envelope(b"not json")[1] == ["invalid_json"]
    assert parse_envelope(b"[1]")[1] == ["not_object"]
    assert parse_envelope(b"")[1] == ["empty_value"]
    _, errs = parse_envelope(env(schema_version=2, event_type="x", payload=[], poll_id="", fetched_at="nope"))
    assert set(errs) == {"bad_schema_version", "unknown_event_type", "missing_payload", "missing_poll_id",
                         "bad_fetched_at"}


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
    assert reason in validate_status_payload(good_status(**over), NOW, 300)


def test_status_validation_accepts_good_and_small_clock_skew():
    assert validate_status_payload(good_status(), NOW, 300) == []
    assert validate_status_payload(good_status(last_reported=int(NOW) + 100), NOW, 300) == []


def test_other_payload_validators():
    assert validate_weather_payload({"current": {"time": 1, "temperature_2m": 3.0}}) == []
    assert validate_weather_payload({}) == ["missing_current"]
    assert validate_poll_log_payload({"job": "x", "outcome": "ok"}) == []
    assert "bad_coordinates" in validate_station_info_payload({"station_id": "a", "lat": "x", "lon": 1})


def series(bikes, docks=None, step=30.0):
    docks = docks or [30 - b for b in bikes]
    return [Obs(NOW - (len(bikes) - 1 - i) * step, b, d) for i, (b, d) in enumerate(zip(bikes, docks))]


def test_depleting_station_is_at_risk_of_empty():
    # 31 observations 30 s apart; the window is (as_of - 15 min, as_of], so the oldest point is excluded
    # and coverage is 14.5 minutes.
    wm = window_metrics(series([10] * 15 + [9] * 8 + [7] * 8), NOW, CFG)          # 10 -> 7
    assert wm["net_change"] == -3 and wm["coverage_min"] == pytest.approx(14.5)
    assert wm["rate_per_min"] == pytest.approx(-3 / 14.5)
    assert wm["minutes_to_empty"] == pytest.approx(7 / (3 / 14.5)) and not wm["at_risk_empty"]   # ~33.8 > 30
    wm = window_metrics(series([10] * 10 + [8] * 10 + [4] * 11), NOW, CFG)        # 10 -> 4
    assert wm["minutes_to_empty"] == pytest.approx(4 / (6 / 14.5)) and wm["at_risk_empty"]        # ~9.7 min


def test_filling_station_is_at_risk_of_full():
    wm = window_metrics(series([20] * 10 + [24] * 10 + [27] * 11), NOW, CFG)      # docks 10 -> 3
    assert wm["at_risk_full"] and wm["minutes_to_full"] == pytest.approx(3 / (7 / 14.5))


def test_noise_floor_and_flat_series_do_not_alert():
    assert not window_metrics(series([5] * 5 + [4] * 26), NOW, CFG)["at_risk_empty"]   # only -1 bike
    wm = window_metrics(series([8] * 31), NOW, CFG)
    assert wm["minutes_to_empty"] is None and not wm["at_risk_empty"] and not wm["at_risk_full"]


def test_short_coverage_gives_no_rate_but_empty_still_reports_zero():
    wm = window_metrics(series([3, 2, 1], step=30.0), NOW, CFG)        # 1 minute of history
    assert wm["rate_per_min"] is None and not wm["at_risk_empty"]
    assert window_metrics(series([0, 0, 0], step=30.0), NOW, CFG)["minutes_to_empty"] == 0.0
    assert window_metrics([], NOW, CFG) is None


def test_observations_outside_window_are_ignored():
    obs = series([5] * 3) + [Obs(NOW - 3600, 30, 0)]
    assert window_metrics(obs, NOW, CFG)["n_obs"] == 3


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
