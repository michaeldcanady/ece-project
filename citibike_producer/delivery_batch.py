"""Delivery results for the messages of one poll."""
from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from confluent_kafka import KafkaError, Message


class DeliveryBatch:
    """Counts delivery results for the messages of one poll."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.sent = 0
        self.delivered = 0
        self.failed = 0
        self.last_error: Optional[str] = None

    def callback(self, err: Optional[KafkaError], msg: Optional[Message]) -> None:
        with self._lock:
            if err is not None:
                self.failed += 1
                self.last_error = str(err)
            else:
                self.delivered += 1
