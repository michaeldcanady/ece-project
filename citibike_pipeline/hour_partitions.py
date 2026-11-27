"""Hour partitions: the key layout of a lake table's hours, shared with the batch logs."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

_HOUR = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})/hr=(\d{2})/")


def partition_path(prefix: str, when: datetime) -> str:
    return "%s/dt=%s/hr=%02d" % (prefix, when.strftime("%Y-%m-%d"), when.hour)


def part_key(prefix: str, when: datetime, name: str) -> str:
    return "%s/%s" % (partition_path(prefix, when), name)


def hour_from_key(key: str) -> Optional[datetime]:
    m = _HOUR.search(key)
    return datetime.strptime("%s %s" % m.groups(), "%Y-%m-%d %H").replace(tzinfo=timezone.utc) if m else None


def floor_hour(when: datetime) -> datetime:
    return when.replace(minute=0, second=0, microsecond=0)


def hours_between(start: datetime, end: datetime) -> Iterable[datetime]:
    cur = floor_hour(start)
    while cur <= end:
        yield cur
        cur += timedelta(hours=1)
