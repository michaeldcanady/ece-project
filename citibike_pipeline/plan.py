from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from .kafka_io import (
    Ranges,
    ranges_from_json,
    ranges_to_json,
)
from .timeutil import iso, parse_iso

log = logging.getLogger(__name__)

STATUS_CONTEXT_COLS = ["station_id", "fetched_at", "last_reported"]


def safe_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


# ---------------------------------------------------------------------------------------------
# records passed between the steps
# ---------------------------------------------------------------------------------------------
# Each record has to_json/from_json so it can travel through Airflow XCom. The JSON keys are the ones the
# steps used to pass as dicts, so values already in flight and the files under ops/batch_plan/ stay readable.
# from_json indexes every field (a missing one raises KeyError), except Published.at_risk: old in-flight XCom lacks it.

JsonObject = Mapping[str, object]


@dataclass(frozen=True)
class Plan:
    """A batch's offset ranges, fixed before anything is read, and the runs that planned and took it."""
    run_id: str
    first_run_id: str
    batch_id: str
    planned_at: datetime
    ranges: Ranges
    total_messages: int

    @property
    def has_data(self) -> bool:
        return self.total_messages > 0

    def to_json(self) -> JsonObject:
        return {"run_id": self.run_id, "first_run_id": self.first_run_id, "batch_id": self.batch_id,
                "planned_at": iso(self.planned_at), "ranges": ranges_to_json(self.ranges),
                "total_messages": self.total_messages}

    @classmethod
    def from_json(cls, obj: JsonObject) -> Plan:
        return cls(run_id=str(obj["run_id"]), first_run_id=str(obj["first_run_id"]), batch_id=str(obj["batch_id"]),
                   planned_at=parse_iso(str(obj["planned_at"])), ranges=ranges_from_json(obj["ranges"]),
                   total_messages=int(obj["total_messages"]))
