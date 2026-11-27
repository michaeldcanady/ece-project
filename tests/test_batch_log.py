"""The Batch log through its interface: record, recent, day."""
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

from citibike_pipeline.batch import Plan, Published, Stats
from citibike_pipeline.batch_log import BatchLog, BatchLogEntry, Counts, Latencies
from citibike_pipeline.local_storage import LocalStorage

T0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)


def entry(batch_id: str, as_of: datetime, **changes) -> BatchLogEntry:
    base = BatchLogEntry(
        batch_id=batch_id, run_id="run_" + batch_id, as_of=as_of, planned_at=as_of,
        published_at=as_of, finished_at=as_of,
        counts=Counts(records_read=10, raw_rows=10, status_rows=8, weather_rows=1, dlq_rows=1,
                      duplicates_dropped=0, repeats=0, out_of_order=0, stale=0, unknown_station=0, replay_rows=0),
        latencies=Latencies(p50_s=1.5, p95_s=2.5, max_s=3.0), status="success")
    return replace(base, **changes)


def make(tmp_path) -> BatchLog:
    return BatchLog(LocalStorage(str(tmp_path)))


def test_empty_lake_gives_empty_results(tmp_path):
    log = make(tmp_path)
    assert log.recent(T0) == []
    assert log.day(date(2026, 3, 10)) == []


def test_record_is_read_back_as_entries(tmp_path):
    log = make(tmp_path)
    e = entry("a", T0, latencies=Latencies(None, None, None), published_at=None)
    log.record(e)
    assert log.recent(T0) == [e]
    assert log.day(T0.date()) == [e]


def test_retry_of_the_same_batch_id_overwrites(tmp_path):
    log = make(tmp_path)
    log.record(entry("a", T0, run_id="first"))
    log.record(entry("a", T0, run_id="retry"))
    assert [e.run_id for e in log.recent(T0)] == ["retry"]
    assert [e.run_id for e in log.day(T0.date())] == ["retry"]


def test_recent_keeps_only_the_last_24_hours(tmp_path):
    log = make(tmp_path)
    log.record(entry("old", T0))
    log.record(entry("edge", T0 + timedelta(hours=10)))
    log.record(entry("new", T0 + timedelta(hours=25)))
    assert [e.batch_id for e in log.recent(T0 + timedelta(hours=25))] == ["edge", "new"]
    # the per-batch history keeps everything
    assert [e.batch_id for e in log.day(T0.date())] == ["old", "edge"]
    assert [e.batch_id for e in log.day((T0 + timedelta(hours=25)).date())] == ["new"]


def test_day_spans_hour_partitions_and_only_that_utc_day(tmp_path):
    log = make(tmp_path)
    midnight = datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc)
    for i, hours in enumerate([-1, 0, 5, 23, 24]):
        log.record(entry("b%d" % i, midnight + timedelta(hours=hours, minutes=30)))
    assert [e.batch_id for e in log.day(date(2026, 3, 10))] == ["b1", "b2", "b3"]


def test_recent_drops_entries_older_than_24_hours_before_now_even_after_an_outage(tmp_path):
    log = make(tmp_path)
    log.record(entry("a", T0))
    assert [e.batch_id for e in log.recent(T0 + timedelta(hours=24))] == ["a"]
    assert log.recent(T0 + timedelta(hours=25)) == []


def test_from_batch_maps_the_step_records_to_an_entry():
    plan = Plan(run_id="r2", first_run_id="r1", batch_id="b1", planned_at=T0, ranges={}, total_messages=3)
    counts = Counts(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    stats = Stats(batch_id="b1", as_of=T0 + timedelta(minutes=1), counts=counts, status_hours=())
    published = Published(published_at=T0 + timedelta(minutes=2), stations=5)
    lat = Latencies(1.0, 2.0, 3.0)
    done = T0 + timedelta(minutes=3)
    assert BatchLogEntry.from_batch(plan, stats, published, lat, done) == BatchLogEntry(
        batch_id="b1", run_id="r2", as_of=stats.as_of, planned_at=T0, published_at=published.published_at,
        finished_at=done, counts=counts, latencies=lat, status="success")


def test_latencies_summarise_sorted_seconds_and_empty_gives_none():
    assert Latencies.from_seconds([1.0, 2.0, 4.0]) == Latencies(p50_s=2.0, p95_s=4.0, max_s=4.0)
    assert Latencies.from_seconds([]) == Latencies(None, None, None)
