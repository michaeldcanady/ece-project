"""Enrichment through its interface: observations in, flagged rows out. No Kafka, no storage."""
from datetime import datetime, timedelta, timezone

from citibike_contract import Envelope
from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.enrichment import PriorContext, enrich_status
from citibike_pipeline.kafka_io import Record
from citibike_pipeline.observations import Delivered, StationStatus
from citibike_pipeline.weather_reading import WeatherReading

CFG = PipelineConfig()
T0 = datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc)
PROCESSED = T0 + timedelta(minutes=1)


def delivered(station="a", last_reported=None, minutes=0, poll_id="p1", offset=0, replay=False, partition=0):
    fetched = T0 + timedelta(minutes=minutes)
    last_reported = int(fetched.timestamp()) - 30 if last_reported is None else last_reported
    status = StationStatus(station_id=station, last_reported=last_reported, num_bikes_available=3,
                           num_docks_available=7, num_ebikes_available=None, num_bikes_disabled=None,
                           num_docks_disabled=None, is_installed=True, is_renting=True, is_returning=True)
    env = Envelope(event_type="station_status", poll_id=poll_id, source="s", source_url="u", gbfs_version="2.3",
                   feed_last_updated=None, fetched_at=fetched, producer_version="0", payload={}, replay=replay)
    return Delivered(Record("t", partition, offset, None, station, b""), env, status)


NO_PRIOR = PriorContext()


def enrich(statuses, prior=NO_PRIOR, dimension=None, weather=()):
    return enrich_status(statuses, prior, dimension, list(weather), "b1", PROCESSED, CFG)


def test_a_fresh_observation_has_no_flags():
    rows, counts = enrich([delivered()])
    assert [(r.is_repeat, r.is_out_of_order, r.is_stale, r.is_unknown_station) for r in rows] == [
        (False, False, False, False)]
    assert (counts.status_rows, counts.repeats, counts.out_of_order, counts.stale) == (1, 0, 0, 0)


def test_the_same_station_and_poll_is_dropped_once_and_counted():
    rows, counts = enrich([delivered(offset=0), delivered(offset=1)])
    assert len(rows) == 1 and counts.duplicates_dropped == 1


def test_a_repeated_last_reported_is_flagged_within_the_batch_and_against_prior_rows():
    rows, counts = enrich([delivered(last_reported=100, poll_id="p1"), delivered(last_reported=100, poll_id="p2",
                                                                                  minutes=1, offset=1)])
    assert [r.is_repeat for r in rows] == [False, True] and counts.repeats == 1
    prior = PriorContext(seen=frozenset({("a", 100)}), max_last_reported={"a": 100})
    rows, _ = enrich([delivered(last_reported=100)], prior=prior)
    assert rows[0].is_repeat


def test_an_older_last_reported_than_already_seen_is_out_of_order():
    prior = PriorContext(seen=frozenset({("a", 200)}), max_last_reported={"a": 200})
    rows, counts = enrich([delivered(last_reported=150)], prior=prior)
    assert rows[0].is_out_of_order and counts.out_of_order == 1


def test_observations_are_processed_in_time_then_offset_order_whatever_the_input_order():
    late = delivered(last_reported=300, minutes=2, poll_id="p2", offset=1)
    early = delivered(last_reported=100, minutes=1, poll_id="p1", offset=0)
    rows, counts = enrich([late, early])
    assert [r.last_reported for r in rows] == [100, 300] and counts.out_of_order == 0


def test_an_observation_older_than_the_stale_threshold_is_stale():
    old = int(T0.timestamp()) - CFG.stale_minutes * 60 - 1
    rows, counts = enrich([delivered(last_reported=old)])
    assert rows[0].is_stale and rows[0].age_s == CFG.stale_minutes * 60 + 1 and counts.stale == 1


def test_a_station_missing_from_the_dimension_is_unknown_and_a_known_one_is_named():
    dimension = {"a": {"name": "Alpha", "lat": 40.7, "lon": -74.0, "capacity": 20}}
    rows, counts = enrich([delivered("a"), delivered("zz", poll_id="p2", offset=1)], dimension=dimension)
    by_station = {r.station_id: r for r in rows}
    assert by_station["a"].station_name == "Alpha" and not by_station["a"].is_unknown_station
    assert by_station["zz"].is_unknown_station and by_station["zz"].station_name is None
    assert counts.unknown_station == 1
    rows, counts = enrich([delivered("zz")], dimension=None)       # no dimension built yet: nothing is unknown
    assert not rows[0].is_unknown_station and counts.unknown_station == 0


def test_a_row_takes_the_latest_weather_at_or_before_its_fetch_time():
    def reading(minutes, temp):
        return WeatherReading(T0 + timedelta(minutes=minutes), temp, None, None, None)
    weather = [reading(5, 20.0), reading(-10, 5.0), reading(-1, 9.0)]            # unsorted on purpose
    rows, _ = enrich([delivered(minutes=0)], weather=weather)
    assert rows[0].temperature_c == 9.0
    rows, _ = enrich([delivered(minutes=0)], weather=[reading(5, 20.0)])           # only a later observation
    assert rows[0].temperature_c is None


def test_replayed_rows_are_counted():
    _, counts = enrich([delivered(replay=True)])
    assert counts.replay_rows == 1
