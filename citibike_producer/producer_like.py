"""The part of a Kafka producer that KafkaSink calls. Fakes and the capture generator's null producer fit it."""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional, Protocol

if TYPE_CHECKING:
    from confluent_kafka import KafkaError, Message


class ProducerLike(Protocol):
    def produce(self, topic: str, *, key: bytes = ..., value: bytes = ...,
                on_delivery: Optional[Callable[[Optional[KafkaError], Optional[Message]], None]] = ...,
                timestamp: int = ...) -> None: ...

    def poll(self, timeout: float = ...) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...
