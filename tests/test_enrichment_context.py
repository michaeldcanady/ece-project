"""Enrichment context through EnrichmentContext.read: earlier lake rows in, the PriorContext and Weather readings
Enrichment reads for a Batch out."""
from datetime import datetime, timedelta, timezone

from citibike_contract import TOPIC_STATUS, TOPIC_WEATHER, Envelope, encode
from citibike_pipeline import lake
from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.decoded_batch import DecodedBatch
from citibike_pipeline.enrichment import PriorContext
from citibike_pipeline.enrichment_context import EnrichmentContext
from citibike_pipeline.kafka_io import Record
from citibike_pipeline.storage import LocalStorage

CFG = PipelineConfig()
BATCH_ID = "b1"
PROCESSED_AT = datetime(2026, 10, 20, 14, 30, tzinfo=timezone.utc)
STATUS_AT = datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc)      # the Batch's first (and only) Station status
WEATHER_AT = datetime(2026, 10, 20, 14, 10, tzinfo=timezone.utc)    # the Batch's Weather fetch, its newest time
WINDOW_START = STATUS_AT - timedelta(minutes=CFG.context_minutes)


def status_payload():
    return {"station_id": "a", "num_bikes_available": 3, "num_docks_available": 7, "num_ebikes_available": 1,
            "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": int(STATUS_AT.timestamp()) - 60}


def weather_payload(obs_time):
    return {"location_id": "nyc", "current": {"time": int(obs_time.timestamp()), "temperature_2m": 12.5,
                                              "precipitation": 0.0, "weather_code": 1, "wind_speed_10m": 9.0}}


def envelope(event_type, fetched_at, payload):
    return encode(Envelope(event_type=event_type, poll_id="p1", source="citibike", source_url="https://gbfs.example",
                           gbfs_version="2.3", feed_last_updated=1_760_968_790, fetched_at=fetched_at,
                           producer_version="0.1", payload=payload))


def status_record(offset=0):
    return Record(topic=TOPIC_STATUS, partition=0, offset=offset, timestamp_ms=None, key="a",
                  value=envelope("station_status", STATUS_AT, status_payload()))


def weather_record(offset=1):
    return Record(topic=TOPIC_WEATHER, partition=0, offset=offset, timestamp_ms=None, key="nyc",
                  value=envelope("weather_current", WEATHER_AT, weather_payload(WEATHER_AT)))


def decode(records):
    return DecodedBatch.decode(records, BATCH_ID, PROCESSED_AT, CFG)


def prior_status(fetched_at, last_reported, offset, batch_id="b0", station_id="a"):
    """A curated status row an earlier batch wrote; only the columns the reads touch carry meaning."""
    return {"station_id": station_id, "fetched_at": fetched_at, "last_reported": last_reported,
            "num_bikes_available": 1, "num_docks_available": 1, "is_installed": True, "is_renting": True,
            "is_returning": True, "age_s": 0, "is_stale": False, "is_repeat": False, "is_out_of_order": False,
            "is_unknown_station": False, "poll_id": "p0", "replay": False, "kafka_topic": TOPIC_STATUS,
            "kafka_partition": 1, "kafka_offset": offset, "batch_id": batch_id, "processed_at": PROCESSED_AT}


def prior_weather(fetched_at, offset, batch_id="b0"):
    """A curated weather row an earlier batch wrote, observed at its fetch time."""
    return {"location_id": "nyc", "obs_time": fetched_at, "fetched_at": fetched_at, "temperature_c": 10.0,
            "precipitation_mm": 0.0, "weather_code": 2, "wind_speed_kmh": 5.0, "poll_id": "p0",
            "kafka_topic": TOPIC_WEATHER, "kafka_partition": 1, "kafka_offset": offset, "batch_id": batch_id,
            "processed_at": PROCESSED_AT}


def obs_times(context):
    return sorted(w.obs_time for w in context.weather)


def test_prior_context_holds_only_rows_inside_the_status_context_window(tmp_path):
    storage = LocalStorage(str(tmp_path))
    lake.CURATED_STATUS.write(storage, "b0", [
        prior_status(STATUS_AT - timedelta(minutes=5), 100, offset=0),            # inside
        prior_status(WINDOW_START, 101, offset=1),                                 # the start edge: inside
        prior_status(STATUS_AT, 102, offset=2),                                    # the end edge: outside
        prior_status(WINDOW_START - timedelta(seconds=1), 103, offset=3),          # before the window
    ])
    context = EnrichmentContext.read(storage, decode([status_record()]), BATCH_ID, CFG)
    assert context.prior.seen == frozenset({("a", 100), ("a", 101)})
    assert context.prior.max_last_reported == {"a": 101}


def test_rows_this_batch_wrote_earlier_are_excluded_before_and_after_compaction(tmp_path):
    storage = LocalStorage(str(tmp_path))
    inside = STATUS_AT - timedelta(minutes=5)
    lake.CURATED_STATUS.write(storage, "b0", [prior_status(inside, 100, offset=0)])
    lake.CURATED_STATUS.write(storage, BATCH_ID, [prior_status(inside, 200, offset=1, batch_id=BATCH_ID)])
    lake.CURATED_WEATHER.write(storage, BATCH_ID, [prior_weather(inside, offset=0, batch_id=BATCH_ID)])
    batch = decode([status_record()])
    before = EnrichmentContext.read(storage, batch, BATCH_ID, CFG)
    lake.CURATED_STATUS.compact_hour(storage, inside.replace(minute=0))
    lake.CURATED_WEATHER.compact_hour(storage, inside.replace(minute=0))
    after = EnrichmentContext.read(storage, batch, BATCH_ID, CFG)
    for context in (before, after):
        assert context.prior.seen == frozenset({("a", 100)})
        assert inside not in obs_times(context)


def test_prior_weather_newer_than_the_first_status_is_read_up_to_the_batchs_newest_time(tmp_path):
    # Partitions drain at different speeds: an earlier batch can hold weather newer than this batch's first status.
    storage = LocalStorage(str(tmp_path))
    late = STATUS_AT + timedelta(minutes=5)
    lake.CURATED_WEATHER.write(storage, "b0", [
        prior_weather(late, offset=0),
        prior_weather(WEATHER_AT, offset=1),                                       # the newest time: inside (+1 ms)
        prior_weather(WEATHER_AT + timedelta(seconds=1), offset=2),                # newer than the Batch: outside
    ])
    context = EnrichmentContext.read(storage, decode([status_record(), weather_record()]), BATCH_ID, CFG)
    assert obs_times(context) == [late, WEATHER_AT, WEATHER_AT]                    # the second is the Batch's own


def test_prior_weather_older_than_the_lookback_is_not_read(tmp_path):
    storage = LocalStorage(str(tmp_path))
    lookback_start = STATUS_AT - timedelta(hours=3)
    lake.CURATED_WEATHER.write(storage, "b0", [
        prior_weather(lookback_start - timedelta(seconds=1), offset=0),
        prior_weather(lookback_start, offset=1),
    ])
    context = EnrichmentContext.read(storage, decode([status_record()]), BATCH_ID, CFG)
    assert obs_times(context) == [lookback_start]


def test_the_batchs_own_weather_observations_are_in_weather(tmp_path):
    storage = LocalStorage(str(tmp_path))
    batch = decode([status_record(), weather_record()])
    context = EnrichmentContext.read(storage, batch, BATCH_ID, CFG)
    assert obs_times(context) == [WEATHER_AT]
    assert context.weather[-1].temperature_c == 12.5


def test_a_batch_without_station_status_has_no_prior_context(tmp_path):
    storage = LocalStorage(str(tmp_path))
    lake.CURATED_STATUS.write(storage, "b0", [prior_status(WEATHER_AT - timedelta(minutes=5), 100, offset=0)])
    context = EnrichmentContext.read(storage, decode([weather_record()]), BATCH_ID, CFG)
    assert context.prior == PriorContext()
    assert obs_times(context) == [WEATHER_AT]


def test_read_is_idempotent(tmp_path):
    storage = LocalStorage(str(tmp_path))
    lake.CURATED_STATUS.write(storage, "b0", [prior_status(STATUS_AT - timedelta(minutes=5), 100, offset=0)])
    lake.CURATED_WEATHER.write(storage, "b0", [prior_weather(STATUS_AT - timedelta(minutes=20), offset=0)])
    batch = decode([status_record(), weather_record()])
    first = EnrichmentContext.read(storage, batch, BATCH_ID, CFG)
    assert EnrichmentContext.read(storage, batch, BATCH_ID, CFG) == first
