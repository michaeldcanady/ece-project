"""Polling jobs. Each poll produces data messages plus one poll_log record.

The producer does not clean or reshape payloads: validation, dedup and
enrichment belong to the Airflow micro-batch. Station dicts are forwarded as received.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Dict, Optional

from .config import (
    Config, TOPIC_INFO, TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER,
)
from .envelope import iso_to_epoch_ms, make_envelope, utc_now_iso
from .sink import DeliveryBatch, KafkaSink
from .sources import (
    FeedRegistry, FetchResult, HttpClient, OUTCOME_HTTP, OUTCOME_PARSE, fetch_weather,
)

log = logging.getLogger(__name__)

SOURCE_GBFS = "citibike_gbfs"
SOURCE_WEATHER = "open_meteo"

OUTCOME_OK = "ok"
OUTCOME_SKIPPED = "skipped_unchanged"
OUTCOME_KAFKA = "kafka_error"
OUTCOME_DISCOVERY = "discovery_error"
_SUCCESS = (OUTCOME_OK, OUTCOME_SKIPPED)


class Job:
    name = "job"

    def __init__(self, cfg: Config, sink: KafkaSink, interval_s: float):
        self.cfg = cfg
        self.sink = sink
        self.interval_s = interval_s
        self.consecutive_failures = 0
        self._last_feed_updated: Optional[int] = None

    def run_once(self) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    def _finish(self, poll_id: str, source: str, url: Optional[str], res: Optional[FetchResult], outcome: str,
                gbfs_version: Optional[str] = None, feed_last_updated: Optional[int] = None,
                feed_ttl: Optional[int] = None, n_records: Optional[int] = None,
                batch: Optional[DeliveryBatch] = None, error: Optional[str] = None) -> bool:
        ok = outcome in _SUCCESS
        self.consecutive_failures = 0 if ok else self.consecutive_failures + 1
        payload: Dict[str, Any] = {
            "job": self.name,
            "outcome": outcome,
            "error": error,
            "started_at": res.started_at if res else utc_now_iso(),
            "fetched_at": res.fetched_at if res else None,
            "http_status": res.http_status if res else None,
            "http_latency_ms": res.latency_ms if res else None,
            "feed_last_updated": feed_last_updated,
            "feed_ttl": feed_ttl,
            "n_records": n_records,
            "messages_sent": batch.sent if batch else 0,
            "messages_delivered": batch.delivered if batch else 0,
            "messages_failed": batch.failed if batch else 0,
            "consecutive_failures": self.consecutive_failures,
        }
        env = make_envelope("poll_log", source, url or "", gbfs_version, feed_last_updated, poll_id,
                            (res.fetched_at if res and res.fetched_at else utc_now_iso()), payload,
                            self.cfg.producer_version)
        self.sink.send(TOPIC_POLL_LOG, self.name, env)
        level = logging.INFO if ok else logging.WARNING
        log.log(level, "%s outcome=%s records=%s latency_ms=%s failures=%d err=%s", self.name, outcome,
                n_records, res.latency_ms if res else None, self.consecutive_failures, error)
        return ok


class GbfsStationJob(Job):
    """station_status (30 s) and station_information (hourly): one message per station."""

    def __init__(self, cfg: Config, sink: KafkaSink, client: HttpClient, registry: FeedRegistry,
                 feed_name: str, event_type: str, topic: str, interval_s: float):
        super().__init__(cfg, sink, interval_s)
        self.name = feed_name
        self.client = client
        self.registry = registry
        self.event_type = event_type
        self.topic = topic

    def run_once(self) -> bool:
        poll_id = uuid.uuid4().hex
        url = self.registry.url_for(self.name)
        if url is None:
            return self._finish(poll_id, SOURCE_GBFS, None, None, OUTCOME_DISCOVERY,
                                error="feed url unavailable from discovery")
        res = self.client.get_json(url)
        if not res.ok:
            if res.outcome == OUTCOME_HTTP and res.http_status in (404, 410):
                self.registry.invalidate()   # feed URL may have moved; rediscover next time
            return self._finish(poll_id, SOURCE_GBFS, url, res, res.outcome, error=res.error)

        body = res.body or {}
        stations = (body.get("data") or {}).get("stations")
        if not isinstance(stations, list):
            return self._finish(poll_id, SOURCE_GBFS, url, res, OUTCOME_PARSE, error="data.stations missing")

        last_updated = body.get("last_updated")
        ttl = body.get("ttl")
        version = body.get("version") or self.registry.version
        common = dict(gbfs_version=version, feed_last_updated=last_updated, feed_ttl=ttl, n_records=len(stations))

        if (self.cfg.skip_unchanged_feed and last_updated is not None
                and last_updated == self._last_feed_updated):
            return self._finish(poll_id, SOURCE_GBFS, url, res, OUTCOME_SKIPPED, **common)

        batch = DeliveryBatch()
        ts = iso_to_epoch_ms(res.fetched_at)
        for st in stations:
            sid = st.get("station_id") if isinstance(st, dict) else None
            key = str(sid) if sid is not None else "_missing_station_id"   # surfaces in downstream DLQ
            env = make_envelope(self.event_type, SOURCE_GBFS, url, version, last_updated, poll_id,
                                res.fetched_at, st, self.cfg.producer_version)
            self.sink.send(self.topic, key, env, batch, ts)
        remaining = self.sink.flush(15.0)
        if batch.failed == 0 and remaining == 0:
            self._last_feed_updated = last_updated
            return self._finish(poll_id, SOURCE_GBFS, url, res, OUTCOME_OK, batch=batch, **common)
        err = batch.last_error or "%d messages undelivered after flush" % remaining
        return self._finish(poll_id, SOURCE_GBFS, url, res, OUTCOME_KAFKA, batch=batch, error=err, **common)


class WeatherJob(Job):
    name = "weather"

    def __init__(self, cfg: Config, sink: KafkaSink, client: HttpClient):
        super().__init__(cfg, sink, cfg.weather_interval_s)
        self.client = client

    def run_once(self) -> bool:
        poll_id = uuid.uuid4().hex
        res = fetch_weather(self.cfg, self.client)
        if not res.ok:
            return self._finish(poll_id, SOURCE_WEATHER, res.url, res, res.outcome, error=res.error)
        body = res.body or {}
        current = body.get("current")
        if not isinstance(current, dict) or "time" not in current:
            return self._finish(poll_id, SOURCE_WEATHER, res.url, res, OUTCOME_PARSE, error="current block missing")

        obs_time = current.get("time")   # epoch seconds (timeformat=unixtime)
        common = dict(feed_last_updated=obs_time, feed_ttl=current.get("interval"), n_records=1)
        if self.cfg.skip_unchanged_feed and obs_time == self._last_feed_updated:
            return self._finish(poll_id, SOURCE_WEATHER, res.url, res, OUTCOME_SKIPPED, **common)

        payload = {
            "location_id": self.cfg.weather_location_id,
            "requested_latitude": self.cfg.weather_lat,
            "requested_longitude": self.cfg.weather_lon,
            "grid_latitude": body.get("latitude"),
            "grid_longitude": body.get("longitude"),
            "current": current,
            "current_units": body.get("current_units"),
        }
        env = make_envelope("weather_current", SOURCE_WEATHER, res.url, None, obs_time, poll_id,
                            res.fetched_at, payload, self.cfg.producer_version)
        batch = DeliveryBatch()
        self.sink.send(TOPIC_WEATHER, self.cfg.weather_location_id, env, batch, iso_to_epoch_ms(res.fetched_at))
        remaining = self.sink.flush(15.0)
        if batch.failed == 0 and remaining == 0:
            self._last_feed_updated = obs_time
            return self._finish(poll_id, SOURCE_WEATHER, res.url, res, OUTCOME_OK, batch=batch, **common)
        err = batch.last_error or "%d messages undelivered after flush" % remaining
        return self._finish(poll_id, SOURCE_WEATHER, res.url, res, OUTCOME_KAFKA, batch=batch, error=err, **common)


def retry_delay(job: Job, cfg: Config) -> float:
    """Delay before the next attempt after a failure: exponential from retry_base_s, capped."""
    base = min(job.interval_s, cfg.retry_base_s)
    cap = min(job.interval_s, cfg.max_backoff_s)
    return min(base * (2 ** max(job.consecutive_failures - 1, 0)), cap)


def run_loop(job: Job, cfg: Config, stop: threading.Event) -> None:
    """Drift-free scheduler. A job exception never kills the loop."""
    next_run = time.monotonic()
    while not stop.is_set():
        try:
            ok = job.run_once()
        except Exception:   # noqa: BLE001 - the loop must survive anything
            log.exception("%s: unhandled error in run_once", job.name)
            job.consecutive_failures += 1
            ok = False
        now = time.monotonic()
        if ok:
            next_run += job.interval_s
            if next_run <= now:   # fell behind (slow poll, suspended host): resume without bursting
                log.warning("%s: behind schedule by %.1fs", job.name, now - next_run)
                next_run = now + 1.0
        else:
            next_run = now + retry_delay(job, cfg)
        stop.wait(timeout=max(next_run - time.monotonic(), 0.0))
