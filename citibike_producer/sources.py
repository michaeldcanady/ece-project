"""HTTP access to GBFS and Open-Meteo. Never raises: failures come back as FetchResult."""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

from .config import Config
from .envelope import utc_now_iso

log = logging.getLogger(__name__)

OUTCOME_OK = "ok"
OUTCOME_TIMEOUT = "timeout"
OUTCOME_CONNECTION = "connection_error"
OUTCOME_HTTP = "http_error"
OUTCOME_PARSE = "parse_error"


@dataclass
class FetchResult:
    outcome: str
    url: str
    started_at: str
    fetched_at: Optional[str] = None
    http_status: Optional[int] = None
    latency_ms: Optional[int] = None
    body: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.outcome == OUTCOME_OK


class HttpClient:
    def __init__(self, cfg: Config, session: Optional[requests.Session] = None):
        self._timeout = (cfg.http_connect_timeout_s, cfg.http_read_timeout_s)
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": cfg.user_agent, "Accept": "application/json"})

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> FetchResult:
        started_at = utc_now_iso()
        t0 = time.monotonic()
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
        except requests.Timeout as exc:
            return FetchResult(OUTCOME_TIMEOUT, url, started_at, error=str(exc)[:300])
        except requests.RequestException as exc:
            return FetchResult(OUTCOME_CONNECTION, url, started_at, error=str(exc)[:300])
        fetched_at = utc_now_iso()
        latency_ms = int((time.monotonic() - t0) * 1000)
        if resp.status_code != 200:
            return FetchResult(OUTCOME_HTTP, url, started_at, fetched_at, resp.status_code,
                               latency_ms, error="HTTP %d" % resp.status_code)
        try:
            body = resp.json()
        except ValueError as exc:
            return FetchResult(OUTCOME_PARSE, url, started_at, fetched_at, resp.status_code,
                               latency_ms, error="invalid JSON: %s" % str(exc)[:200])
        if not isinstance(body, dict):
            return FetchResult(OUTCOME_PARSE, url, started_at, fetched_at, resp.status_code,
                               latency_ms, error="JSON root is not an object")
        return FetchResult(OUTCOME_OK, url, started_at, fetched_at, resp.status_code, latency_ms, body)


class FeedRegistry:
    """Resolves GBFS feed URLs from the discovery file (gbfs.json) and caches them."""

    def __init__(self, cfg: Config, client: HttpClient):
        self._cfg = cfg
        self._client = client
        self._lock = threading.Lock()
        self._urls: Dict[str, str] = {}
        self._version: Optional[str] = None
        self._loaded_at = 0.0

    @property
    def version(self) -> Optional[str]:
        return self._version

    def url_for(self, feed_name: str) -> Optional[str]:
        with self._lock:
            stale = (time.monotonic() - self._loaded_at) > self._cfg.discovery_refresh_s
            if feed_name in self._urls and not stale:
                return self._urls[feed_name]
        self.refresh()
        with self._lock:
            return self._urls.get(feed_name)

    def invalidate(self) -> None:
        """Force rediscovery on next use (e.g. after a 404 on a feed URL)."""
        with self._lock:
            self._loaded_at = 0.0

    def refresh(self) -> bool:
        res = self._client.get_json(self._cfg.gbfs_discovery_url)
        if not res.ok:
            log.warning("GBFS discovery failed: %s %s", res.outcome, res.error)
            return False
        urls = parse_discovery(res.body, self._cfg.gbfs_language)
        if not urls:
            log.warning("GBFS discovery returned no feeds")
            return False
        with self._lock:
            self._urls = urls
            self._version = res.body.get("version")
            self._loaded_at = time.monotonic()
        log.info("GBFS discovery ok: version=%s feeds=%s", self._version, sorted(urls))
        return True


def parse_discovery(body: Dict[str, Any], language: str) -> Dict[str, str]:
    data = body.get("data") or {}
    lang = data.get(language) or (next(iter(data.values())) if data else None)
    feeds = (lang or {}).get("feeds") or []
    return {f["name"]: f["url"] for f in feeds if "name" in f and "url" in f}


WEATHER_VARIABLES = (
    "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,"
    "rain,snowfall,weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m"
)


def fetch_weather(cfg: Config, client: HttpClient) -> FetchResult:
    params = {
        "latitude": cfg.weather_lat,
        "longitude": cfg.weather_lon,
        "current": WEATHER_VARIABLES,
        "timeformat": "unixtime",   # unambiguous UTC epoch seconds
        "timezone": "UTC",
    }
    return client.get_json(cfg.open_meteo_url, params=params)
