"""The contract's poll log payload: PollLog and Outcome, across the producer / pipeline seam."""
import gzip
import json
import os
from datetime import datetime, timezone

import pytest

from citibike_contract import (
    OUTCOMES,
    Envelope,
    InvalidPollLog,
    PollLog,
    decode,
    encode,
    is_success,
    kafka_key,
)

SAMPLE = os.path.join(os.path.dirname(__file__), "..", "data", "sample", "sample_capture.jsonl.gz")


def full(**kw):
    return PollLog(job="station_status", outcome="ok", started_at="2026-10-20T12:00:00.000+00:00",
                   fetched_at="2026-10-20T12:00:00.100+00:00", http_status=200, http_latency_ms=80,
                   feed_last_updated=1792497600, feed_ttl=60, n_records=3, messages_sent=3,
                   messages_delivered=3, **kw)


def test_poll_log_round_trips_through_the_wire():
    log = full(consecutive_failures=2, error="boom")
    env = Envelope(event_type="poll_log", poll_id="p1", source="citibike_gbfs", source_url="u", gbfs_version=None,
                   feed_last_updated=None, fetched_at=datetime(2026, 10, 20, 12, tzinfo=timezone.utc),
                   producer_version="t", payload=dict(log.to_payload()))
    assert PollLog.from_payload(decode(encode(env)).payload) == log
    assert kafka_key(env) == "station_status"


def test_every_poll_log_in_the_sample_capture_decodes_and_keeps_the_keys_it_had():
    seen = 0
    with gzip.open(SAMPLE, "rb") as fh:
        for line in fh:
            env = json.loads(line)
            if env["event_type"] != "poll_log":
                continue
            seen += 1
            decoded = PollLog.from_payload(env["payload"]).to_payload()      # older captures lack newer keys
            assert {k: decoded[k] for k in env["payload"]} == env["payload"]
    assert seen > 0


def test_payload_key_order_is_the_wire_order():
    assert list(full().to_payload()) == [
        "job", "outcome", "error", "started_at", "fetched_at", "http_status", "http_latency_ms",
        "feed_last_updated", "feed_ttl", "n_records", "messages_sent", "messages_delivered", "messages_failed",
        "messages_skipped", "consecutive_failures"]


def test_a_sparse_payload_decodes_with_defaults():
    log = PollLog.from_payload({"job": "weather", "outcome": "timeout"})
    assert log.http_status is None and log.messages_sent == 0 and not log.ok


@pytest.mark.parametrize("payload,reasons", [
    ({"outcome": "ok"}, ["bad_job"]),
    ({"job": "x"}, ["bad_outcome"]),
    ({"job": "x", "outcome": "banana"}, ["bad_outcome"]),
    ({"job": 1, "outcome": None}, ["bad_job", "bad_outcome"]),
    (["x"], ["bad_job", "bad_outcome"]),
])
def test_an_invalid_poll_log_names_every_failed_check(payload, reasons):
    with pytest.raises(InvalidPollLog) as exc:
        PollLog.from_payload(payload)
    assert exc.value.reasons == reasons


def test_only_ok_and_skipped_unchanged_are_successes():
    assert {o for o in OUTCOMES if is_success(o)} == {"ok", "skipped_unchanged"}
    assert len(OUTCOMES) == 8
