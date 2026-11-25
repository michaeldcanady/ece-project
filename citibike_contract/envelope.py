"""The envelope: the message the producer sends for every observation, poll or replayed record.

Both sides use this module for everything they must agree on: the fields, the bytes on the wire
(encode / decode) and the Kafka key. Standard library only.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Dict, List, Optional

from citibike_contract import KNOWN_EVENT_TYPES, SCHEMA_VERSION, iso, parse_iso


class InvalidEnvelope(ValueError):
    """The bytes are not a valid envelope. `reasons` lists every failed check, as DLQ reason codes:
    empty_value, invalid_json, not_object, bad_schema_version, unknown_event_type, missing_payload,
    missing_poll_id, bad_fetched_at, bad_replayed_at. The message is the codes joined by commas."""

    def __init__(self, reasons: List[str]) -> None:
        super().__init__(",".join(reasons))
        self.reasons = list(reasons)


@dataclass(frozen=True)
class Envelope:
    event_type: str
    poll_id: str
    source: Optional[str]
    source_url: Optional[str]
    gbfs_version: Optional[str]
    feed_last_updated: Optional[int]   # source timestamp (epoch seconds)
    fetched_at: datetime               # ingest timestamp (UTC, producer clock)
    producer_version: Optional[str]
    payload: Dict[str, Any]
    replay: bool = False
    replayed_at: Optional[datetime] = None

    def mark_replayed(self, replayed_at: datetime) -> Envelope:
        """A copy flagged as replayed at `replayed_at`; the fetch time (event time) is kept."""
        return replace(self, replay=True, replayed_at=replayed_at)


def kafka_key(envelope: Envelope) -> str:
    """The key an envelope is sent under. Live jobs and replay must agree: the station information
    topic is compacted, so a different key for the same station would keep both versions."""
    payload = envelope.payload
    if envelope.event_type in ("station_status", "station_information"):
        sid = payload.get("station_id")
        return str(sid) if sid is not None else "_missing_station_id"   # surfaces in downstream DLQ
    if envelope.event_type == "weather_current":
        location_id = payload.get("location_id")
        return str(location_id) if location_id is not None else "_missing_location_id"
    if envelope.event_type == "poll_log":
        return str(payload.get("job") or "poll_log")
    raise ValueError("no Kafka key rule for event type %r" % envelope.event_type)


def encode(envelope: Envelope) -> bytes:
    """Compact JSON in the wire's field order; the replay fields only for a replayed envelope."""
    wire: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_type": envelope.event_type,
        "poll_id": envelope.poll_id,
        "source": envelope.source,
        "source_url": envelope.source_url,
        "gbfs_version": envelope.gbfs_version,
        "feed_last_updated": envelope.feed_last_updated,
        "fetched_at": iso(envelope.fetched_at),
        "producer_version": envelope.producer_version,
        "payload": envelope.payload,
    }
    if envelope.replay:
        wire["replay"] = True
        wire["replayed_at"] = iso(envelope.replayed_at)
    return json.dumps(wire, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def decode(value: Optional[bytes]) -> Envelope:
    """Parse wire bytes into an envelope, or raise InvalidEnvelope with every reason they are not one.
    Unknown top-level fields are dropped, so envelopes from a newer producer still decode."""
    if not value:
        raise InvalidEnvelope(["empty_value"])
    try:
        env = json.loads(value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise InvalidEnvelope(["invalid_json"]) from None
    if not isinstance(env, dict):
        raise InvalidEnvelope(["not_object"])

    errors: List[str] = []
    if env.get("schema_version") != SCHEMA_VERSION:
        errors.append("bad_schema_version")
    if env.get("event_type") not in KNOWN_EVENT_TYPES:
        errors.append("unknown_event_type")
    if not isinstance(env.get("payload"), dict):
        errors.append("missing_payload")
    if not env.get("poll_id"):
        errors.append("missing_poll_id")
    fetched_at = _parse_timestamp(env.get("fetched_at"))
    if fetched_at is None:
        errors.append("bad_fetched_at")
    replayed_at = None
    if env.get("replayed_at") is not None:
        replayed_at = _parse_timestamp(env["replayed_at"])
        if replayed_at is None:
            errors.append("bad_replayed_at")
    if errors:
        raise InvalidEnvelope(errors)

    return Envelope(
        event_type=env["event_type"],
        poll_id=env["poll_id"],
        source=env.get("source"),
        source_url=env.get("source_url"),
        gbfs_version=env.get("gbfs_version"),
        feed_last_updated=env.get("feed_last_updated"),
        fetched_at=fetched_at,
        producer_version=env.get("producer_version"),
        payload=env["payload"],
        replay=bool(env.get("replay", False)),
        replayed_at=replayed_at,
    )


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        return parse_iso(value)
    except ValueError:
        return None
