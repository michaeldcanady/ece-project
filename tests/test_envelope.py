"""Seam 1: the contract's envelope interface. Bytes in, envelopes or reasons out; envelopes in, bytes or keys out."""
import gzip
import json
import os
from datetime import datetime, timezone

import pytest

from citibike_contract import Envelope, decode
from citibike_contract.envelope import InvalidEnvelope, encode, kafka_key

SAMPLE = os.path.join(os.path.dirname(__file__), "..", "data", "sample", "sample_capture.jsonl.gz")


def test_every_sample_capture_line_decodes_and_reencodes_to_the_same_bytes():
    with gzip.open(SAMPLE, "rb") as fh:
        lines = [line.rstrip(b"\n") for line in fh if line.strip()]
    assert len(lines) == 1937
    for line in lines:
        assert encode(decode(line)) == line


GOOD = {
    "schema_version": 1, "event_type": "station_status", "poll_id": "p1", "source": "citibike_gbfs",
    "source_url": "https://example.test/station_status.json", "gbfs_version": "2.3",
    "feed_last_updated": 1792497600, "fetched_at": "2026-10-20T12:00:00.000+00:00",
    "producer_version": "test", "payload": {"station_id": "st-1"},
}


_DROP = object()   # an override that removes the field


def _wire(**overrides):
    env = {**GOOD, **overrides}
    return json.dumps({k: v for k, v in env.items() if v is not _DROP}).encode("utf-8")


def _reasons(value):
    with pytest.raises(InvalidEnvelope) as exc:
        decode(value)
    return exc.value.reasons


@pytest.mark.parametrize("value, reason", [
    (b"", "empty_value"),
    (None, "empty_value"),
    (b"{not json", "invalid_json"),
    (b"\xff\xfe", "invalid_json"),
    (b"[1, 2]", "not_object"),
    (_wire(schema_version=2), "bad_schema_version"),
    (_wire(schema_version=_DROP), "bad_schema_version"),
    (_wire(event_type="station_gossip"), "unknown_event_type"),
    (_wire(payload=_DROP), "missing_payload"),
    (_wire(payload=[1]), "missing_payload"),
    (_wire(poll_id=""), "missing_poll_id"),
    (_wire(poll_id=_DROP), "missing_poll_id"),
    (_wire(fetched_at="yesterday"), "bad_fetched_at"),
    (_wire(fetched_at=_DROP), "bad_fetched_at"),
    (_wire(fetched_at=12345), "bad_fetched_at"),
    (_wire(replay=True, replayed_at="not a time"), "bad_replayed_at"),
    (_wire(replay=True, replayed_at=7), "bad_replayed_at"),
])
def test_each_bad_input_is_rejected_with_its_reason(value, reason):
    assert _reasons(value) == [reason]


def test_every_failed_check_is_reported_at_once():
    value = _wire(schema_version=9, event_type="nope", payload=None, poll_id=None, fetched_at="x",
                  replayed_at="y")
    assert _reasons(value) == ["bad_schema_version", "unknown_event_type", "missing_payload",
                               "missing_poll_id", "bad_fetched_at", "bad_replayed_at"]


def test_invalid_envelope_is_a_value_error_whose_message_lists_the_reasons():
    with pytest.raises(ValueError, match="^missing_poll_id,bad_fetched_at$"):
        decode(_wire(poll_id="", fetched_at="x"))


FETCHED = datetime(2026, 10, 20, 12, 0, 0, 123000, tzinfo=timezone.utc)
REPLAYED = datetime(2026, 11, 1, 9, 30, 0, 456000, tzinfo=timezone.utc)


def _envelope(event_type="station_status", payload=None):
    return Envelope(
        event_type=event_type, poll_id="p1", source="citibike_gbfs", source_url="https://example.test/f.json",
        gbfs_version="2.3", feed_last_updated=1792497600, fetched_at=FETCHED, producer_version="test",
        payload={"station_id": "st-1"} if payload is None else payload,
    )


def test_a_live_envelope_encodes_in_wire_order_without_replay_fields():
    assert encode(_envelope()) == (
        b'{"schema_version":1,"event_type":"station_status","poll_id":"p1","source":"citibike_gbfs",'
        b'"source_url":"https://example.test/f.json","gbfs_version":"2.3","feed_last_updated":1792497600,'
        b'"fetched_at":"2026-10-20T12:00:00.123+00:00","producer_version":"test","payload":{"station_id":"st-1"}}'
    )


def test_non_ascii_payload_text_is_written_as_is():
    assert b'"name":"Caf\xc3\xa9"' in encode(_envelope(payload={"name": "Café"}))


def test_a_replayed_envelope_round_trips_keeping_its_fetch_time_and_the_original_is_unchanged():
    original = _envelope()
    replayed = original.mark_replayed(REPLAYED)
    assert original.replay is False and original.replayed_at is None
    wire = encode(replayed)
    assert wire.endswith(b'"payload":{"station_id":"st-1"},"replay":true,'
                         b'"replayed_at":"2026-11-01T09:30:00.456+00:00"}')
    back = decode(wire)
    assert back == replayed
    assert back.replay is True and back.replayed_at == REPLAYED and back.fetched_at == FETCHED


def test_an_unknown_top_level_field_is_dropped_on_decode():
    env = decode(_wire(added_by_a_newer_producer={"x": 1}))
    assert encode(env) == json.dumps(GOOD, separators=(",", ":")).encode("utf-8")


@pytest.mark.parametrize("event_type, payload, key", [
    ("station_status", {"station_id": "st-1"}, "st-1"),
    ("station_status", {"station_id": 72}, "72"),
    ("station_status", {}, "_missing_station_id"),
    ("station_information", {"station_id": "st-2"}, "st-2"),
    ("station_information", {"station_id": None}, "_missing_station_id"),
    ("weather_current", {"location_id": "nyc"}, "nyc"),
    ("weather_current", {}, "_missing_location_id"),
    ("poll_log", {"job": "station_status"}, "station_status"),
])
def test_kafka_key_for_each_event_type(event_type, payload, key):
    assert kafka_key(_envelope(event_type, payload)) == key
