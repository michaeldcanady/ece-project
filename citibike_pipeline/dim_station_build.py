"""What one build of the station dimension did: the figures the DAG task returns."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping


@dataclass(frozen=True)
class DimStationBuild:
    stations: int
    records_read: int
    rejected: int
    messages_planned: int
    ranges: str     # the planned offset ranges as JSON

    def to_json(self) -> Mapping[str, object]:
        return asdict(self)
