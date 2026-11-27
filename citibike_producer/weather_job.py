"""The weather job: one Open-Meteo poll."""
from __future__ import annotations

import logging
import uuid
from typing import Mapping

from citibike_contract import (
    OUTCOME_KAFKA,
    OUTCOME_OK,
    OUTCOME_PARSE,
    OUTCOME_SKIPPED,
    Envelope,
    MissingKafkaKey,
    iso_to_epoch_ms,
    kafka_key,
    parse_iso,
)

from .config import TOPIC_WEATHER, Config
from .delivery_batch import DeliveryBatch
from .http_client import HttpClient
from .job import Job
from .jsonvalues import optional_int
from .kafka_sink import KafkaSink
from .weather_source import fetch_weather

log = logging.getLogger(__name__)


SOURCE_WEATHER = "open_meteo"


class WeatherJob(Job):
    name = "weather"

    def __init__(self, cfg: Config, sink: KafkaSink, client: HttpClient):
        super().__init__(cfg, sink)
        self.client = client

    def run_once(self) -> bool:
        poll_id = uuid.uuid4().hex
        res = fetch_weather(self.cfg, self.client)
        if not res.ok:
            return self._finish(poll_id, SOURCE_WEATHER, res.url, res, res.outcome, error=res.error)
        body = res.body or {}
        current = body.get("current")
        if not isinstance(current, Mapping) or "time" not in current:
            return self._finish(poll_id, SOURCE_WEATHER, res.url, res, OUTCOME_PARSE, error="current block missing")

        obs_time = optional_int(current.get("time"))   # epoch seconds (timeformat=unixtime)
        common = dict(feed_last_updated=obs_time, feed_ttl=optional_int(current.get("interval")), n_records=1)
        if (self.cfg.skip_unchanged_feed and obs_time is not None
                and obs_time == self._last_feed_updated):
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
        env = Envelope(event_type="weather_current", poll_id=poll_id, source=SOURCE_WEATHER, source_url=res.url,
                       gbfs_version=None, feed_last_updated=obs_time, fetched_at=parse_iso(res.fetched_at),
                       producer_version=self.cfg.producer_version, payload=payload)
        try:
            key = kafka_key(env)
        except MissingKafkaKey as exc:
            return self._finish(poll_id, SOURCE_WEATHER, res.url, res, OUTCOME_PARSE, error=str(exc),
                                messages_skipped=1, **common)
        batch = DeliveryBatch()
        self.sink.send(TOPIC_WEATHER, key, env, batch, iso_to_epoch_ms(res.fetched_at))
        remaining = self.sink.flush(15.0)
        if batch.failed == 0 and remaining == 0:
            self._last_feed_updated = obs_time
            return self._finish(poll_id, SOURCE_WEATHER, res.url, res, OUTCOME_OK, batch=batch, **common)
        err = batch.last_error or "%d messages undelivered after flush" % remaining
        return self._finish(poll_id, SOURCE_WEATHER, res.url, res, OUTCOME_KAFKA, batch=batch, error=err, **common)
