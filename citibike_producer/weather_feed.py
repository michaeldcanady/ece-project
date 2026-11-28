"""The Open-Meteo current weather as a poll source."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .config import TOPIC_WEATHER, Config
from .feed_read import FeedRead
from .fetch_result import FetchResult
from .http_client import HttpClient
from .jsonvalues import optional_int
from .unreadable_feed import UnreadableFeed
from .weather_source import fetch_weather

SOURCE_WEATHER = "open_meteo"


@dataclass(frozen=True)
class WeatherFeed:
    cfg: Config
    client: HttpClient

    @property
    def job(self) -> str:
        return "weather"

    @property
    def source(self) -> str:
        return SOURCE_WEATHER

    @property
    def event_type(self) -> str:
        return "weather_current"

    @property
    def topic(self) -> str:
        return TOPIC_WEATHER

    @property
    def skips_keyless_envelopes(self) -> bool:
        return False

    def fetch(self) -> FetchResult:
        return fetch_weather(self.cfg, self.client)

    def read(self, res: FetchResult) -> FeedRead:
        body = res.body or {}
        current = body.get("current")
        if not isinstance(current, Mapping) or "time" not in current:
            raise UnreadableFeed("current block missing")
        payload = {
            "location_id": self.cfg.weather_location_id,
            "requested_latitude": self.cfg.weather_lat,
            "requested_longitude": self.cfg.weather_lon,
            "grid_latitude": body.get("latitude"),
            "grid_longitude": body.get("longitude"),
            "current": current,
            "current_units": body.get("current_units"),
        }
        return FeedRead(feed_last_updated=optional_int(current.get("time")),   # epoch seconds (timeformat=unixtime)
                        feed_ttl=optional_int(current.get("interval")), gbfs_version=None, payloads=[payload])
