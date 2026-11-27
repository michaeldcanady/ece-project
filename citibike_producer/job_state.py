"""What a polling job carries between runs: the last feed timestamp (for skip-unchanged) and the failure count."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional

from citibike_contract import JsonObject

from .jsonvalues import optional_int


@dataclass(frozen=True)
class JobState:
    last_feed_updated: Optional[int] = None
    consecutive_failures: int = 0

    def to_json(self) -> JsonObject:
        return {"last_feed_updated": self.last_feed_updated, "consecutive_failures": self.consecutive_failures}

    @staticmethod
    def from_json(obj: object) -> JobState:
        """The state, or a fresh one when `obj` is missing or malformed."""
        if not isinstance(obj, Mapping):
            return JobState()
        return JobState(last_feed_updated=optional_int(obj.get("last_feed_updated")),
                        consecutive_failures=optional_int(obj.get("consecutive_failures")) or 0)
