"""The part of a Kafka producer that KafkaSink calls. Fakes and the capture generator's null producer fit it."""
from __future__ import annotations

from typing import Callable, Optional, Protocol


class ProducerLike(Protocol):
    def produce(self, topic: str, *, key: bytes = ..., value: bytes = ...,
                on_delivery: Optional[Callable[[object, object], None]] = ..., timestamp: int = ...) -> None: ...

    def poll(self, timeout: float = ...) -> object: ...

    def flush(self, timeout: float = ...) -> int: ...
