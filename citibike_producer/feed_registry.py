"""Resolves GBFS feed URLs from the discovery file (gbfs.json) and caches them."""
from __future__ import annotations

import logging
import threading
import time
from typing import Mapping, MutableMapping, Optional

from citibike_contract import (
    JsonObject,
)

from .config import Config
from .discovery_state import DiscoveryState
from .http_client import HttpClient
from .jsonvalues import mapping_or_empty, optional_str

log = logging.getLogger(__name__)


class FeedRegistry:
    """Resolves GBFS feed URLs from the discovery file (gbfs.json) and caches them."""

    def __init__(self, cfg: Config, client: HttpClient):
        self._cfg = cfg
        self._client = client
        self._lock = threading.Lock()
        self._urls: Mapping[str, str] = {}
        self._version: Optional[str] = None
        self._loaded_at = 0.0

    @property
    def version(self) -> Optional[str]:
        return self._version

    def url_for(self, feed_name: str) -> Optional[str]:
        with self._lock:
            stale = (time.time() - self._loaded_at) > self._cfg.discovery_refresh_s
            if feed_name in self._urls and not stale:
                return self._urls[feed_name]
        self.refresh()
        with self._lock:
            return self._urls.get(feed_name)

    def state(self) -> DiscoveryState:
        """The cached discovery result, so the next task run can skip re-fetching gbfs.json."""
        with self._lock:
            return DiscoveryState(urls=dict(self._urls), version=self._version, loaded_at=self._loaded_at)

    def restore(self, state: Optional[DiscoveryState]) -> None:
        if state is None or not state.urls:
            return
        with self._lock:
            self._urls = dict(state.urls)
            self._version = state.version
            self._loaded_at = state.loaded_at

    def invalidate(self) -> None:
        """Force rediscovery on next use (e.g. after a 404 on a feed URL)."""
        with self._lock:
            self._loaded_at = 0.0

    def refresh(self) -> bool:
        res = self._client.get_json(self._cfg.gbfs_discovery_url)
        if not res.ok:
            log.warning("GBFS discovery failed: %s %s", res.outcome, res.error)
            return False
        body = res.body or {}
        urls = parse_discovery(body, self._cfg.gbfs_language)
        if not urls:
            log.warning("GBFS discovery returned no feeds")
            return False
        with self._lock:
            self._urls = urls
            self._version = optional_str(body.get("version"))
            self._loaded_at = time.time()
        log.info("GBFS discovery ok: version=%s feeds=%s", self._version, sorted(urls))
        return True


def parse_discovery(body: JsonObject, language: str) -> Mapping[str, str]:
    data = mapping_or_empty(body.get("data"))
    lang = data.get(language) or (next(iter(data.values())) if data else None)
    feeds = mapping_or_empty(lang).get("feeds")
    urls: MutableMapping[str, str] = {}
    for feed in feeds if isinstance(feeds, list) else []:
        if isinstance(feed, Mapping) and "name" in feed and "url" in feed:
            urls[str(feed["name"])] = str(feed["url"])
    return urls
