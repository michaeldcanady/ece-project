import gzip
import json
import threading

from citibike_producer.config import TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER, Config
from citibike_producer.jobs import GbfsStationJob, WeatherJob
from citibike_producer.replay import replay
from citibike_producer.sink import KafkaSink
from citibike_producer.sources import FetchResult, parse_discovery
from citibike_producer.tasks import poll_once, replay_capture


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
    job = GbfsStationJob(cfg, sink, client, FakeRegistry(), "station_status", "station_status", TOPIC_STATUS)
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


DISCOVERY_BODY = {"version": "2.3", "data": {"en": {"feeds": [
    {"name": "station_status", "url": "https://example.test/station_status.json"},
    {"name": "station_information", "url": "https://example.test/station_information.json"}]}}}


def test_poll_once_carries_state_between_runs():
    """Each Airflow run is a fresh process: discovery and skip-unchanged must survive via the state dict."""
    prod = FakeProducer()
    first = poll_once("station_status", None, Config(), producer=prod,
                      client=FakeClient([ok_result(DISCOVERY_BODY), ok_result(STATUS_BODY)]))
    assert first["ok"] and first["outcome"] == "ok"
    assert first["state"]["job"]["last_feed_updated"] == 1760000100
    assert first["state"]["discovery"]["urls"]["station_status"].endswith("station_status.json")
    # second run: no discovery fetch (the client has only the status response), unchanged feed skipped
    second = poll_once("station_status", first["state"], Config(), producer=prod,
                       client=FakeClient([ok_result(STATUS_BODY)]))
    assert second["ok"] and second["outcome"] == "skipped_unchanged"
    assert len(by_topic(prod, TOPIC_STATUS)) == 3


def test_poll_once_reports_failure_and_counts_it_in_state():
    bad = FetchResult("timeout", "u", "2026-10-20T14:00:00.000+00:00", error="read timeout")
    state = {"job": {"consecutive_failures": 2}}
    prod = FakeProducer()
    res = poll_once("weather", state, Config(), producer=prod, client=FakeClient([bad]))
    assert not res["ok"] and res["outcome"] == "timeout" and res["error"] == "read timeout"
    assert res["state"]["job"]["consecutive_failures"] == 3
    assert by_topic(prod, TOPIC_POLL_LOG)[-1][2]["payload"]["consecutive_failures"] == 3


def test_poll_once_captures_one_file_per_job(tmp_path):
    cfg = Config(capture_dir=str(tmp_path / "capture"))
    poll_once("station_status", None, cfg, producer=FakeProducer(),
              client=FakeClient([ok_result(DISCOVERY_BODY), ok_result(STATUS_BODY)]))
    with gzip.open(tmp_path / "capture" / "station_status.jsonl.gz", "rt") as fh:
        assert len(fh.readlines()) == 4   # 3 stations + poll_log


def test_replay_merges_a_capture_directory_in_event_time_order(tmp_path):
    def write(name, times):
        with gzip.open(tmp_path / name, "wt") as fh:
            for t in times:
                fh.write(json.dumps({"schema_version": 1, "event_type": "poll_log", "poll_id": t,
                                     "fetched_at": "2026-10-20T14:00:%s.000+00:00" % t,
                                     "payload": {"job": name}}) + "\n")
    write("a.jsonl.gz", ["01", "04"])
    write("b.jsonl.gz", ["02", "03"])
    prod = FakeProducer()
    assert replay_capture(str(tmp_path), speed=0, cfg=Config(), producer=prod) == 4
    assert [m[2]["poll_id"] for m in prod.sent] == ["01", "02", "03", "04"]


def test_ensure_topics_is_idempotent_and_raises_on_real_errors():
    from citibike_producer.create_topics import ensure_topics

    class Fut:
        def __init__(self, exc=None):
            self.exc = exc

        def result(self):
            if self.exc:
                raise self.exc

    class Admin:
        def __init__(self, errors):
            self.errors = errors

        def create_topics(self, new):
            return {t.topic: Fut(self.errors.get(t.topic)) for t in new}

    out = ensure_topics("x", admin=Admin({TOPIC_STATUS: Exception("TOPIC_ALREADY_EXISTS")}))
    assert out[TOPIC_STATUS] == "exists" and out[TOPIC_POLL_LOG] == "created"
    import pytest
    with pytest.raises(RuntimeError, match=TOPIC_WEATHER):
        ensure_topics("x", admin=Admin({TOPIC_WEATHER: Exception("POLICY_VIOLATION")}))


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
                         "station_status", "station_status", TOPIC_STATUS)
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


def test_synthetic_rejects_too_few_stations(tmp_path):
    import pytest

    from citibike_producer.synthetic import generate
    with pytest.raises(ValueError):
        generate(str(tmp_path / "x.jsonl.gz"), stations=5, minutes=1)


def test_synthetic_overwrites_an_existing_capture(tmp_path):
    from citibike_producer.synthetic import generate
    path = str(tmp_path / "x.jsonl.gz")
    generate(path, stations=8, minutes=2)
    with gzip.open(path, "rt") as fh:
        first = len(fh.readlines())
    generate(path, stations=8, minutes=2)
    with gzip.open(path, "rt") as fh:
        assert len(fh.readlines()) == first
