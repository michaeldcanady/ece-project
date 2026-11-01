"""Configuration, read from environment variables (prefix CB_) with safe defaults."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

# Topic names are part of the pipeline contract; re-exported here for the producer modules.
from citibike_contract import (  # noqa: F401
    TOPIC_BY_EVENT_TYPE,
    TOPIC_DLQ,
    TOPIC_INFO,
    TOPIC_POLL_LOG,
    TOPIC_STATUS,
    TOPIC_WEATHER,
)


def _bool(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


def _opt(name: str) -> Optional[str]:
    value = os.environ.get(name, "").strip()
    return value or None


@dataclass(frozen=True)
class Config:
    bootstrap_servers: str = "localhost:9092"

    # GBFS: feed URLs are discovered from this file, never hardcoded.
    gbfs_discovery_url: str = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"
    gbfs_language: str = "en"
    discovery_refresh_s: float = 6 * 3600

    # Open-Meteo: one Midtown Manhattan coordinate (configurable).
    open_meteo_url: str = "https://api.open-meteo.com/v1/forecast"
    weather_lat: float = 40.7549
    weather_lon: float = -73.9840
    weather_location_id: str = "manhattan-midtown"

    http_connect_timeout_s: float = 5.0
    http_read_timeout_s: float = 10.0

    # If the feed's own last_updated has not advanced, the content is identical.
    skip_unchanged_feed: bool = True

    producer_version: str = "dev"
    user_agent: str = "ece5874-citibike-pipeline/0.1 (Virginia Tech course project)"

    # Optional capture of every envelope for a real sample dataset: one gzip JSONL file per job under
    # this directory, so concurrently running ingest tasks never append to the same file.
    capture_dir: Optional[str] = None
    # Single capture file for one sink (set internally from capture_dir, and by the synthetic generator).
    tee_path: Optional[str] = None

    @classmethod
    def from_env(cls) -> Config:
        d = cls()
        return cls(
            bootstrap_servers=os.environ.get("CB_BOOTSTRAP_SERVERS", d.bootstrap_servers),
            gbfs_discovery_url=os.environ.get("CB_GBFS_DISCOVERY_URL", d.gbfs_discovery_url),
            gbfs_language=os.environ.get("CB_GBFS_LANGUAGE", d.gbfs_language),
            discovery_refresh_s=float(os.environ.get("CB_DISCOVERY_REFRESH_S", d.discovery_refresh_s)),
            open_meteo_url=os.environ.get("CB_OPEN_METEO_URL", d.open_meteo_url),
            weather_lat=float(os.environ.get("CB_WEATHER_LAT", d.weather_lat)),
            weather_lon=float(os.environ.get("CB_WEATHER_LON", d.weather_lon)),
            weather_location_id=os.environ.get("CB_WEATHER_LOCATION_ID", d.weather_location_id),
            http_connect_timeout_s=float(os.environ.get("CB_HTTP_CONNECT_TIMEOUT_S", d.http_connect_timeout_s)),
            http_read_timeout_s=float(os.environ.get("CB_HTTP_READ_TIMEOUT_S", d.http_read_timeout_s)),
            skip_unchanged_feed=_bool(os.environ.get("CB_SKIP_UNCHANGED_FEED", str(d.skip_unchanged_feed))),
            producer_version=os.environ.get("CB_PRODUCER_VERSION", os.environ.get("GIT_SHA", d.producer_version)),
            user_agent=os.environ.get("CB_USER_AGENT", d.user_agent),
            capture_dir=_opt("CB_CAPTURE_DIR"),
        )
