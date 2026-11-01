"""The producer and the pipeline read the same CB_* variables and share one contract."""
from datetime import timezone

from citibike_contract import (
    EVENT_TYPE_BY_TOPIC,
    KNOWN_EVENT_TYPES,
    TOPIC_BY_EVENT_TYPE,
    iso,
    iso_to_epoch_ms,
    parse_iso,
)
from citibike_pipeline.config import PipelineConfig
from citibike_producer.config import Config


def test_shared_env_vars_parse_in_both_configs(monkeypatch):
    monkeypatch.setenv("CB_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("CB_STATUS_INTERVAL_S", "60.0")
    producer, pipeline = Config.from_env(), PipelineConfig.from_env()
    assert producer.bootstrap_servers == pipeline.bootstrap_servers == "broker:9092"
    assert pipeline.status_interval_s == 60.0


def test_pipeline_config_from_env_overrides(monkeypatch):
    monkeypatch.setenv("CB_STORAGE", "s3")
    monkeypatch.setenv("CB_S3_ENDPOINT_URL", "")
    monkeypatch.setenv("CB_NOISE_FLOOR_BIKES", "3")
    cfg = PipelineConfig.from_env()
    assert cfg.storage == "s3" and cfg.s3_endpoint_url is None and cfg.noise_floor_bikes == 3


def test_event_type_maps_are_inverse():
    assert {t: et for et, t in TOPIC_BY_EVENT_TYPE.items()} == EVENT_TYPE_BY_TOPIC
    assert set(KNOWN_EVENT_TYPES) == set(TOPIC_BY_EVENT_TYPE)


def test_timestamps_round_trip_and_naive_means_utc():
    dt = parse_iso("2026-10-20T14:15:00.123+02:00")
    assert dt.tzinfo == timezone.utc and iso(dt) == "2026-10-20T12:15:00.123+00:00"
    assert iso_to_epoch_ms("2026-10-20T12:15:00.123") == iso_to_epoch_ms("2026-10-20T12:15:00.123+00:00")
