"""Everything one poll hands to the next run of the same task. The DAG keeps its JSON form in XCom."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Optional

from citibike_contract import JsonObject

from .discovery_state import DiscoveryState
from .job_state import JobState


@dataclass(frozen=True)
class PollState:
    job: JobState = field(default_factory=JobState)
    discovery: Optional[DiscoveryState] = None      # the weather job does not use GBFS discovery

    def to_json(self) -> JsonObject:
        out = {"job": self.job.to_json()}
        return out if self.discovery is None else {**out, "discovery": self.discovery.to_json()}

    @staticmethod
    def from_json(obj: object) -> PollState:
        """The state, or a fresh one when `obj` is missing or malformed (the first run has none)."""
        if not isinstance(obj, Mapping):
            return PollState()
        return PollState(job=JobState.from_json(obj.get("job")),
                         discovery=DiscoveryState.from_json(obj.get("discovery")))
