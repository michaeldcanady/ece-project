"""What a source read from a good response."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass(frozen=True)
class FeedRead:
    feed_last_updated: Optional[int]
    feed_ttl: Optional[int]
    gbfs_version: Optional[str]
    payloads: Sequence[object]      # one per envelope; forwarded unchanged, the producer does not clean payloads

    @property
    def n_records(self) -> int:
        return len(self.payloads)
