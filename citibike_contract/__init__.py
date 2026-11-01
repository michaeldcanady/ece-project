"""The producer <-> pipeline contract: topic names, event types, envelope schema version, and the
UTC timestamp format. Standard library only, so both the producer image (no pyarrow) and the
Airflow image can import it."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional

SCHEMA_VERSION = 1

TOPIC_STATUS = "citibike.station_status"
TOPIC_INFO = "citibike.station_info"
TOPIC_WEATHER = "weather.current"
TOPIC_POLL_LOG = "pipeline.poll_log"
TOPIC_DLQ = "pipeline.dlq"   # created for operators; the pipeline's own DLQ is the lake's dlq/ prefix

# Envelope event_type -> topic. Used by live jobs, replay, and the pipeline's topic/type check.
TOPIC_BY_EVENT_TYPE: Dict[str, str] = {
    "station_status": TOPIC_STATUS,
    "station_information": TOPIC_INFO,
    "weather_current": TOPIC_WEATHER,
    "poll_log": TOPIC_POLL_LOG,
}
EVENT_TYPE_BY_TOPIC: Dict[str, str] = {t: et for et, t in TOPIC_BY_EVENT_TYPE.items()}
KNOWN_EVENT_TYPES = tuple(TOPIC_BY_EVENT_TYPE)


# Timestamps on the wire are ISO-8601 UTC with millisecond precision, e.g. 2026-10-20T14:15:00.123+00:00.
# A timestamp without an offset is read as UTC.

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat(timespec="milliseconds") if dt else None


def utc_now_iso() -> str:
    return utcnow().isoformat(timespec="milliseconds")


def parse_iso(value: str) -> datetime:
    """Parse the producer's ISO-8601 timestamps (3.9-safe)."""
    dt = datetime.fromisoformat(value)
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def iso_to_epoch_ms(value: str) -> int:
    return int(parse_iso(value).timestamp() * 1000)
