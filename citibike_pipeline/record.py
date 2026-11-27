"""One Kafka message as the reader returns it."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass


@dataclass
class Record:
    topic: str
    partition: int
    offset: int
    timestamp_ms: Optional[int]
    key: Optional[str]
    value: bytes
