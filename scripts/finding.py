"""One license-policy violation."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Finding:
    """The package that violates the policy, and what the maintainer must do."""

    package: str
    message: str
