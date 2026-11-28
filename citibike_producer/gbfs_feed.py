"""The GBFS station feeds as a poll source: station_status (every minute) and station_information (hourly)."""
from __future__ import annotations

from dataclasses import dataclass

from citibike_contract import OUTCOME_DISCOVERY, OUTCOME_HTTP, utc_now_iso

from .config import Config
from .feed_read import FeedRead
from .feed_registry import FeedRegistry
from .fetch_result import FetchResult
from .http_client import HttpClient
from .station_feed import StationFeed
from .unreadable_feed import UnreadableFeed

SOURCE_GBFS = "citibike_gbfs"


@dataclass(frozen=True)
class GbfsFeed:
    cfg: Config
    client: HttpClient
    registry: FeedRegistry
    job: str            # the feed name in the discovery file
    event_type: str
    topic: str

    @property
    def source(self) -> str:
        return SOURCE_GBFS

    @property
    def skips_keyless_envelopes(self) -> bool:
        return True

    def fetch(self) -> FetchResult:
        url = self.registry.url_for(self.job)
        if url is None:
            return FetchResult(OUTCOME_DISCOVERY, "", utc_now_iso(), error="feed url unavailable from discovery")
        res = self.client.get_json(url)
        if res.outcome == OUTCOME_HTTP and res.http_status in (404, 410):
            self.registry.invalidate()   # feed URL may have moved; rediscover next time
        return res

    def read(self, res: FetchResult) -> FeedRead:
        feed = StationFeed.parse(res.body or {})
        if feed is None:
            raise UnreadableFeed("data.stations missing")
        return FeedRead(feed_last_updated=feed.last_updated, feed_ttl=feed.ttl,
                        gbfs_version=feed.version or self.registry.version, payloads=feed.stations)
