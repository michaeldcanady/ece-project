from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

# {"ok": ok, "outcome": j.last_outcome, "error": j.last_error, "state": new_state}

@dataclass(frozen=True)
class PollResult:
    """The result of a poll"""
    ok: bool
    state: Dict[str, Any]
    outcome: Optional[str] = None
    error: Optional[str] = None
