"""Reading values out of a parsed JSON feed: each function narrows an `object` and never raises."""
from __future__ import annotations

from typing import Mapping, Optional


def optional_int(value: object) -> Optional[int]:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def optional_str(value: object) -> Optional[str]:
    return value if isinstance(value, str) else None


def mapping_or_empty(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}
