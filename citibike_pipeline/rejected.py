"""A payload that failed its parse, with every reason it failed."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class Rejected:
    """A payload that failed its parse, with every reason it failed."""
    reasons: Tuple[str, ...]
