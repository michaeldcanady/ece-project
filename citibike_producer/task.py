from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .poll_state import PollState


@dataclass(frozen=True)
class PollResult:
    """What one poll returns to its task.

    `ok` is False for a failed poll, which is already recorded in pipeline.poll_log: the caller saves `state`, then
    decides whether to fail the task. `outcome` and `error` are the poll log's outcome code and message.
    """
    ok: bool
    state: PollState
    outcome: Optional[str] = None
    error: Optional[str] = None
