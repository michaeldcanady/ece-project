from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .poll_state import PollState


@dataclass(frozen=True)
class PollResult:
    """The result of a poll"""
    ok: bool
    state: PollState
    outcome: Optional[str] = None
    error: Optional[str] = None
