"""The outcome of one HTTP fetch of a feed."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from citibike_contract import (
    OUTCOME_OK,
    JsonObject,
    Outcome,
)


@dataclass
class FetchResult:
    outcome: Outcome
    url: str
    started_at: str
    fetched_at: Optional[str] = None
    http_status: Optional[int] = None
    latency_ms: Optional[int] = None
    body: Optional[JsonObject] = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.outcome == OUTCOME_OK
