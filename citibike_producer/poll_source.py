"""What one source contributes to a poll: where to fetch, how to read the body, and how to publish it."""
from __future__ import annotations

from typing import Protocol

from .feed_read import FeedRead
from .fetch_result import FetchResult


class PollSource(Protocol):
    @property
    def job(self) -> str:
        """The poll log's job name and its Kafka key."""

    @property
    def source(self) -> str: ...

    @property
    def event_type(self) -> str:
        """The event type of the data envelopes."""

    @property
    def topic(self) -> str: ...

    @property
    def skips_keyless_envelopes(self) -> bool:
        """True: an envelope with no Kafka key is skipped and counted. False: it fails the poll."""

    def fetch(self) -> FetchResult:
        """Never raises: a failed fetch is a FetchResult with an outcome."""

    def read(self, res: FetchResult) -> FeedRead:
        """Raises UnreadableFeed when the body lacks what the source needs."""
