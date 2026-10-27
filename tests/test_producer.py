import gzip
import json
import threading

from citibike_producer.config import Config, TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER
from citibike_producer.jobs import GbfsStationJob, WeatherJob, retry_delay, run_loop
from citibike_producer.replay import replay
from citibike_producer.sink import KafkaSink
from citibike_producer.sources import FetchResult, parse_discovery


class FakeProducer:
    """Mimics confluent_kafka.Producer: delivery callbacks fire on flush()."""

    def __init__(self, fail=False):
        self.sent = []
        self._cbs = []
        self.fail = fail

    def produce(self, topic, key=None, value=None, on_delivery=None, timestamp=None):
        self.sent.append((topic, key.decode(), json.loads(value), timestamp))
        if on_delivery:
            self._cbs.append(on_delivery)

    def poll(self, t=0):
        return 0

    def flush(self, t=0):
        for cb in self._cbs:
            cb("boom" if self.fail else None, None)
        self._cbs = []
        return 0


def station(i, last_reported=1760000000):
    return {"station_id": str(i), "num_bikes_available": 3, "num_docks_available": 10,
            "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": last_reported}


STATUS_BODY = {"last_updated": 1760000100, "ttl": 5, "version": "2.3",
               "data": {"stations": [station(1), station(2), station(3)]}}


class FakeClient:
    def __init__(self, results):
        self.results = list(results)

    def get_json(self, url, params=None):
        return self.results.pop(0)


class FakeRegistry:
    version = "2.3"
    invalidated = 0

    def url_for(self, name):
        return "https://example.test/%s.json" % name

    def invalidate(self):
        self.invalidated += 1


def ok_result(body):
    return FetchResult("ok", "https://example.test/x.json", "2026-10-20T14:00:00.000+00:00",
                       "2026-10-20T14:00:00.500+00:00", 200, 120, body)


def make_job(client, producer, cfg=None):
    cfg = cfg or Config()
    sink = KafkaSink(cfg, producer=producer)
    job = GbfsStationJob(cfg, sink, client, FakeRegistry(), "station_status", "station_status",
                         TOPIC_STATUS, cfg.status_interval_s)
    return job


def by_topic(producer, topic):
    return [m for m in producer.sent if m[0] == topic]


def test_status_poll_publishes_one_message_per_station_and_poll_log():
    prod = FakeProducer()
    job = make_job(FakeClient([ok_result(STATUS_BODY)]), prod)
    assert job.run_once() is True
    msgs = by_topic(prod, TOPIC_STATUS)
    assert [m[1] for m in msgs] == ["1", "2", "3"]
    env = msgs[0][2]
    assert env["event_type"] == "station_status" and env["gbfs_version"] == "2.3"
    assert env["feed_last_updated"] == 1760000100 and env["payload"]["station_id"] == "1"
    assert len({m[2]["poll_id"] for m in msgs}) == 1
    log = by_topic(prod, TOPIC_POLL_LOG)[0][2]["payload"]
    assert log["outcome"] == "ok" and log["messages_delivered"] == 3 and log["n_records"] == 3


def test_unchanged_feed_is_skipped_but_logged():
    prod = FakeProducer()
    job = make_job(FakeClient([ok_result(STATUS_BODY), ok_result(STATUS_BODY)]), prod)
    job.run_once()
    n = len(by_topic(prod, TOPIC_STATUS))
    assert job.run_once() is True
    assert len(by_topic(prod, TOPIC_STATUS)) == n
    assert by_topic(prod, TOPIC_POLL_LOG)[-1][2]["payload"]["outcome"] == "skipped_unchanged"


def test_http_failure_logs_and_counts_failures_and_resets_on_success():
    prod = FakeProducer()
    bad = FetchResult("timeout", "u", "2026-10-20T14:00:00.000+00:00", error="read timeout")
    job = make_job(FakeClient([bad, bad, ok_result(STATUS_BODY)]), prod)
    assert job.run_once() is False and job.run_once() is False
    assert job.consecutive_failures == 2
    assert by_topic(prod, TOPIC_POLL_LOG)[-1][2]["payload"]["outcome"] == "timeout"
    assert job.run_once() is True and job.consecutive_failures == 0


def test_404_triggers_rediscovery():
    prod = FakeProducer()
    nf = FetchResult("http_error", "u", "2026-10-20T14:00:00.000+00:00", http_status=404, error="HTTP 404")
    job = make_job(FakeClient([nf]), prod)
    job.run_once()
    assert job.registry.invalidated == 1


def test_kafka_delivery_failure_is_reported_and_not_marked_seen():
    prod = FakeProducer(fail=True)
    job = make_job(FakeClient([ok_result(STATUS_BODY), ok_result(STATUS_BODY)]), prod)
    assert job.run_once() is False
    assert by_topic(prod, TOPIC_POLL_LOG)[-1][2]["payload"]["outcome"] == "kafka_error"
    job.run_once()   # same last_updated must be republished, not skipped
    assert len(by_topic(prod, TOPIC_STATUS)) == 6


def test_malformed_body_is_parse_error():
    prod = FakeProducer()
    job = make_job(FakeClient([ok_result({"data": {}})]), prod)
    assert job.run_once() is False
    assert by_topic(prod, TOPIC_POLL_LOG)[-1][2]["payload"]["outcome"] == "parse_error"


def test_station_without_id_is_forwarded_with_sentinel_key():
    prod = FakeProducer()
    body = {"last_updated": 1, "version": "2.3", "data": {"stations": [{"num_bikes_available": 1}]}}
    make_job(FakeClient([ok_result(body)]), prod).run_once()
    assert by_topic(prod, TOPIC_STATUS)[0][1] == "_missing_station_id"


def test_weather_job():
    prod = FakeProducer()
    cfg = Config()
    body = {"latitude": 40.75, "longitude": -73.98,
            "current_units": {"temperature_2m": "°C"},
            "current": {"time": 1760000000, "interval": 900, "temperature_2m": 14.2, "precipitation": 0.0}}
    job = WeatherJob(cfg, KafkaSink(cfg, producer=prod), FakeClient([ok_result(body), ok_result(body)]))
    assert job.run_once() is True
    msg = by_topic(prod, TOPIC_WEATHER)[0]
    assert msg[1] == cfg.weather_location_id
    assert msg[2]["payload"]["current"]["temperature_2m"] == 14.2 and msg[2]["feed_last_updated"] == 1760000000
    job.run_once()
    assert len(by_topic(prod, TOPIC_WEATHER)) == 1   # unchanged observation skipped


def test_retry_delay_backs_off_and_caps():
    cfg = Config()
    job = make_job(FakeClient([]), FakeProducer(), cfg)
    delays = []
    for f in range(1, 6):
        job.consecutive_failures = f
        delays.append(retry_delay(job, cfg))
    # status interval is 30 s, so the retry delay is capped at the interval
    assert delays == [30, 30, 30, 30, 30]
    info = GbfsStationJob(cfg, KafkaSink(cfg, producer=FakeProducer()), None, FakeRegistry(),
                          "station_information", "station_information", "t", cfg.info_interval_s)
    got = []
    for f in range(1, 8):
        info.consecutive_failures = f
        got.append(retry_delay(info, cfg))
    assert got == [30, 60, 120, 120, 120, 120, 120]


def test_run_loop_survives_exceptions_and_stops():
    class Boom:
        name = "boom"
        interval_s = 0.01
        consecutive_failures = 0
        calls = 0

        def run_once(self):
            self.calls += 1
            raise RuntimeError("x")

    cfg = Config(retry_base_s=0.01, max_backoff_s=0.02)
    job, stop = Boom(), threading.Event()
    t = threading.Thread(target=run_loop, args=(job, cfg, stop))
    t.start()
    threading.Timer(0.3, stop.set).start()
    t.join(timeout=5)
    assert not t.is_alive() and job.calls >= 2


def test_replay_preserves_event_time_and_marks_replay(tmp_path):
    envs = []
    for i, fa in enumerate(["2026-10-20T14:00:00.000+00:00", "2026-10-20T14:00:30.000+00:00"]):
        envs.append({"schema_version": 1, "event_type": "station_status", "poll_id": "p%d" % i,
                     "fetched_at": fa, "payload": station(7)})
    envs.append({"schema_version": 1, "event_type": "poll_log", "poll_id": "p1",
                 "fetched_at": "2026-10-20T14:00:31.000+00:00", "payload": {"job": "station_status"}})
    path = tmp_path / "sample.jsonl.gz"
    with gzip.open(path, "wt") as fh:
        for e in envs:
            fh.write(json.dumps(e) + "\n")
        fh.write("not json\n")
    prod = FakeProducer()
    cfg = Config()
    n = replay(str(path), KafkaSink(cfg, producer=prod), cfg, threading.Event(), speed=0)
    assert n == 3
    topics = [m[0] for m in prod.sent]
    assert topics == [TOPIC_STATUS, TOPIC_STATUS, TOPIC_POLL_LOG]
    sent = prod.sent[0][2]
    assert sent["fetched_at"] == envs[0]["fetched_at"] and sent["replay"] is True and "replayed_at" in sent
    assert prod.sent[0][3] is None   # broker timestamp, not original CreateTime


def test_tee_writes_gzip_jsonl(tmp_path):
    cfg = Config(tee_path=str(tmp_path / "tee.jsonl.gz"))
    prod = FakeProducer()
    sink = KafkaSink(cfg, producer=prod)
    job = GbfsStationJob(cfg, sink, FakeClient([ok_result(STATUS_BODY)]), FakeRegistry(),
                         "station_status", "station_status", TOPIC_STATUS, 30)
    job.run_once()
    sink.close()
    with gzip.open(cfg.tee_path, "rt") as fh:
        lines = [json.loads(x) for x in fh]
    assert len(lines) == 4   # 3 stations + poll_log


def test_parse_discovery():
    body = {"data": {"en": {"feeds": [{"name": "station_status", "url": "https://a/s"},
                                       {"name": "station_information", "url": "https://a/i"}]}}}
    assert parse_discovery(body, "en") == {"station_status": "https://a/s", "station_information": "https://a/i"}
    assert parse_discovery(body, "fr")["station_status"] == "https://a/s"   # falls back to first language
    assert parse_discovery({}, "en") == {}
