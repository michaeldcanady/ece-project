"""The GBFS station jobs: station_status (every minute) and station_information (hourly).

The producer does not clean or reshape payloads: validation, dedup and enrichment belong to the Airflow
micro-batch. Station items are forwarded as received."""
from __future__ import annotations

import logging
import uuid
from typing import Mapping

from citibike_contract import (
    OUTCOME_DISCOVERY,
    OUTCOME_HTTP,
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

from .config import Config
from .delivery_batch import DeliveryBatch
from .feed_registry import FeedRegistry
from .http_client import HttpClient
from .job import Job
from .kafka_sink import KafkaSink
from .station_feed import StationFeed

log = logging.getLogger(__name__)


SOURCE_GBFS = "citibike_gbfs"


class GbfsStationJob(Job):
    """station_status (every minute) and station_information (hourly): one message per station."""

    def __init__(self, cfg: Config, sink: KafkaSink, client: HttpClient, registry: FeedRegistry,
                 feed_name: str, event_type: str, topic: str):
        super().__init__(cfg, sink)
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

        feed = StationFeed.parse(res.body or {})
        if feed is None:
            return self._finish(poll_id, SOURCE_GBFS, url, res, OUTCOME_PARSE, error="data.stations missing")

        last_updated = feed.last_updated
        version = feed.version or self.registry.version
        common = dict(gbfs_version=version, feed_last_updated=last_updated, feed_ttl=feed.ttl,
                      n_records=len(feed.stations))

        if (self.cfg.skip_unchanged_feed and last_updated is not None
                and last_updated == self._last_feed_updated):
            return self._finish(poll_id, SOURCE_GBFS, url, res, OUTCOME_SKIPPED, **common)

        batch = DeliveryBatch()
        ts = iso_to_epoch_ms(res.fetched_at)
        fetched_at = parse_iso(res.fetched_at)
        skipped = 0
        for st in feed.stations:
            if not isinstance(st, Mapping):
                log.warning("%s: skipping a station item that is not an object", self.name)
                skipped += 1
                continue
            env = Envelope(event_type=self.event_type, poll_id=poll_id, source=SOURCE_GBFS, source_url=url,
                           gbfs_version=version, feed_last_updated=last_updated, fetched_at=fetched_at,
                           producer_version=self.cfg.producer_version, payload=st)
            try:
                key = kafka_key(env)
            except MissingKafkaKey as exc:
                log.warning("%s: skipping a station: %s", self.name, exc)
                skipped += 1
                continue
            self.sink.send(self.topic, key, env, batch, ts)
        remaining = self.sink.flush(15.0)
        if batch.failed == 0 and remaining == 0:
            self._last_feed_updated = last_updated
            return self._finish(poll_id, SOURCE_GBFS, url, res, OUTCOME_OK, batch=batch,
                                messages_skipped=skipped, **common)
        err = batch.last_error or "%d messages undelivered after flush" % remaining
        return self._finish(poll_id, SOURCE_GBFS, url, res, OUTCOME_KAFKA, batch=batch, error=err,
                            messages_skipped=skipped, **common)
