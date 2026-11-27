"""The result of compacting one hour of a lake table."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CompactHour:
    """The result of compacting one hour of a lake table: how many source files were merged, and how many rows remain"""
    sources: int
    rows: int
