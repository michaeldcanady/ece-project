"""What one poll returns: its outcome, and the state the next poll of the same source starts from."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from citibike_contract import Outcome, is_success

from .job_state import JobState


@dataclass(frozen=True)
class PollOutcome:
    outcome: Outcome
    error: Optional[str]
    state: JobState

    @property
    def ok(self) -> bool:
        return is_success(self.outcome)
