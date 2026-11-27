"""The raw row: one decoded Kafka record, its envelope provenance and its payload as received.

Fields are in `schemas.RAW` order; tests/test_rows.py checks they match it.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from citibike_contract import Envelope

from .record import Record
from .timeutil import from_epoch_ms


@dataclass(frozen=True)
class RawRow:
    """A record whose envelope decoded on the topic its event type belongs to, valid payload or not."""
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp: Optional[datetime]
    event_type: str
    poll_id: str
    source: Optional[str]
    source_url: Optional[str]
    gbfs_version: Optional[str]
    feed_last_updated: Optional[int]
    fetched_at: datetime
    producer_version: Optional[str]
    replay: bool
    replayed_at: Optional[datetime]
    payload_json: str
    batch_id: str
    processed_at: datetime

    @classmethod
    def from_delivery(cls, record: Record, envelope: Envelope, batch_id: str, processed_at: datetime) -> RawRow:
        return cls(kafka_topic=record.topic, kafka_partition=record.partition, kafka_offset=record.offset,
                   kafka_timestamp=from_epoch_ms(record.timestamp_ms), event_type=envelope.event_type,
                   poll_id=envelope.poll_id, source=envelope.source, source_url=envelope.source_url,
                   gbfs_version=envelope.gbfs_version, feed_last_updated=envelope.feed_last_updated,
                   fetched_at=envelope.fetched_at, producer_version=envelope.producer_version,
                   replay=envelope.replay, replayed_at=envelope.replayed_at,
                   payload_json=json.dumps(envelope.payload, separators=(",", ":")),
                   batch_id=batch_id, processed_at=processed_at)
