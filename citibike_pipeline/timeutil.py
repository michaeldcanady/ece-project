"""Time helpers. The wire format (utcnow, iso, parse_iso) is defined once in citibike_contract."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from citibike_contract import iso, parse_iso, utcnow  # noqa: F401


def from_epoch(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, timezone.utc)


def epoch(dt: datetime) -> float:
    return dt.timestamp()


def floor_5min(dt: datetime) -> datetime:
    return dt.replace(minute=dt.minute - dt.minute % 5, second=0, microsecond=0)


def from_epoch_ms(ms: Optional[int]) -> Optional[datetime]:
    """The time of a Kafka timestamp in milliseconds, or None when the record carries none."""
    return from_epoch(ms / 1000.0) if ms is not None else None
