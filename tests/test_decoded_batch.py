"""Batch decode through DecodedBatch.decode: Kafka records in; raw rows, dead letters, observations, as_of out."""
from datetime import datetime, timezone

from citibike_contract import TOPIC_STATUS, TOPIC_WEATHER, Envelope, encode
from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.decoded_batch import DecodedBatch
from citibike_pipeline.record import Record
from citibike_pipeline.timeutil import from_epoch_ms

CFG = PipelineConfig()
PROCESSED_AT = datetime(2026, 10, 20, 14, 30, tzinfo=timezone.utc)
STATUS_AT = datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc)
WEATHER_AT = datetime(2026, 10, 20, 14, 10, tzinfo=timezone.utc)


def status_payload(**over):
    p = {"station_id": "a", "num_bikes_available": 3, "num_docks_available": 7, "num_ebikes_available": 1,
         "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": int(STATUS_AT.timestamp()) - 60}
    p.update(over)
    return p


def weather_payload():
    return {"location_id": "nyc", "current": {"time": int(WEATHER_AT.timestamp()), "temperature_2m": 12.5,
                                              "precipitation": 0.0, "weather_code": 1, "wind_speed_10m": 9.0}}


def envelope(event_type="station_status", fetched_at=STATUS_AT, payload=None):
    return encode(Envelope(event_type=event_type, poll_id="p1", source="citibike", source_url="https://gbfs.example",
                           gbfs_version="2.3", feed_last_updated=1_760_968_790, fetched_at=fetched_at,
                           producer_version="0.1", payload=status_payload() if payload is None else payload))


def record(value, topic=TOPIC_STATUS, offset=0, timestamp_ms=None):
    return Record(topic=topic, partition=0, offset=offset, timestamp_ms=timestamp_ms, key="a", value=value)


def decode(records):
    return DecodedBatch.decode(records, "b1", PROCESSED_AT, CFG)


def test_as_of_is_the_newest_status_fetch_even_when_weather_is_newer():
    batch = decode([record(envelope(), offset=0),
                    record(envelope("weather_current", WEATHER_AT, weather_payload()), topic=TOPIC_WEATHER, offset=1)])
    assert len(batch.status) == 1 and len(batch.weather) == 1
    assert batch.as_of == STATUS_AT


def test_as_of_without_decodable_records_is_the_newest_kafka_timestamp():
    batch = decode([record(b"not json", offset=0, timestamp_ms=1_760_968_000_000),
                    record(b"{", offset=1, timestamp_ms=1_760_968_500_000)])
    assert batch.raw_rows == ()
    assert batch.as_of == from_epoch_ms(1_760_968_500_000)


def test_as_of_without_decodable_records_or_kafka_timestamps_is_processed_at():
    assert decode([record(b"not json")]).as_of == PROCESSED_AT
    assert decode([]).as_of == PROCESSED_AT


def test_a_record_on_the_wrong_topic_for_its_event_type_is_dead_lettered_without_a_raw_row():
    batch = decode([record(envelope(), topic=TOPIC_WEATHER, offset=4)])
    assert batch.raw_rows == ()
    assert [d.reason for d in batch.dead_letters] == ["event_type_topic_mismatch"]
    assert batch.dead_letters[0].kafka_offset == 4 and batch.dead_letters[0].kafka_topic == TOPIC_WEATHER


def test_a_rejected_status_yields_a_raw_row_and_a_dead_letter_with_the_joined_reasons():
    batch = decode([record(envelope(payload=status_payload(station_id="", num_bikes_available=-1)))])
    assert len(batch.raw_rows) == 1
    assert batch.status == ()
    assert [d.reason for d in batch.dead_letters] == ["bad_station_id,bad_num_bikes_available"]
    assert batch.dead_letters[0].raw_value == record(envelope(payload=status_payload(
        station_id="", num_bikes_available=-1))).value.decode("utf-8")


def test_raw_rows_by_event_type_groups_the_raw_rows():
    batch = decode([record(envelope(), offset=0),
                    record(envelope("weather_current", WEATHER_AT, weather_payload()), topic=TOPIC_WEATHER, offset=1)])
    groups = batch.raw_rows_by_event_type()
    assert {et: [r.kafka_offset for r in rows] for et, rows in groups.items()} == {
        "station_status": [0], "weather_current": [1]}
    assert batch.records_read == 2
