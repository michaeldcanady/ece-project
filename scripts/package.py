"""The license metadata of one installed package."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class Package:
    """One installed distribution and the three places its metadata can name a license."""

    name: str
    version: str
    license_expression: Optional[str]  # License-Expression
    license_text: Optional[str]  # License
    classifiers: Tuple[str, ...]  # "License ::" entries
