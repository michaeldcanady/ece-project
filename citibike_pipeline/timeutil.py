"""Time helpers. The wire format (utcnow, iso, parse_iso) is defined once in citibike_contract."""
from __future__ import annotations

from datetime import datetime, timezone

from citibike_contract import iso, parse_iso, utcnow  # noqa: F401


def from_epoch(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, timezone.utc)


def epoch(dt: datetime) -> float:
    return dt.timestamp()


def partition_path(prefix: str, dt: datetime) -> str:
    return "%s/dt=%s/hr=%02d" % (prefix, dt.strftime("%Y-%m-%d"), dt.hour)


def floor_5min(dt: datetime) -> datetime:
    return dt.replace(minute=dt.minute - dt.minute % 5, second=0, microsecond=0)
