"""The dead-letter row: a Kafka record the pipeline could not use, the reasons why and its bytes as received.

Fields are in `schemas.DLQ` order; tests/test_rows.py checks they match it.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from .record import Record
from .timeutil import from_epoch_ms


@dataclass(frozen=True)
class DeadLetterRow:
    """One rejected record; `reason` is its reason codes joined by commas."""
    reason: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp: Optional[datetime]
    raw_value: str
    batch_id: str
    processed_at: datetime

    @classmethod
    def from_record(cls, record: Record, reasons: Sequence[str], batch_id: str,
                    processed_at: datetime) -> DeadLetterRow:
        return cls(reason=",".join(reasons), kafka_topic=record.topic, kafka_partition=record.partition,
                   kafka_offset=record.offset, kafka_timestamp=from_epoch_ms(record.timestamp_ms),
                   raw_value=(record.value or b"").decode("utf-8", "replace"),
                   batch_id=batch_id, processed_at=processed_at)
