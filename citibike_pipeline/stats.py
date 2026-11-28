from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

from citibike_contract import JsonObject

from .batch_log import Counts
from .jsonfields import as_array
from .timeutil import iso, parse_iso

log = logging.getLogger(__name__)

STATUS_CONTEXT_COLS = ["station_id", "fetched_at", "last_reported"]

@dataclass(frozen=True)
class Stats:
    """What process_batch counted and wrote for a batch. The JSON keeps the counts flat."""
    batch_id: str
    as_of: datetime
    counts: Counts
    status_hours: Tuple[datetime, ...]

    def to_json(self) -> JsonObject:
        return {"batch_id": self.batch_id, "as_of": iso(self.as_of), **self.counts.to_json(),
                "status_hours": [iso(h) for h in self.status_hours]}

    @classmethod
    def from_json(cls, obj: JsonObject) -> Stats:
        return cls(batch_id=str(obj["batch_id"]), as_of=parse_iso(str(obj["as_of"])), counts=Counts.from_json(obj),
                   status_hours=tuple(parse_iso(str(h)) for h in as_array(obj["status_hours"])))
