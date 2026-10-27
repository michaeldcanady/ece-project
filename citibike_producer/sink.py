"""Kafka sink: idempotent producer, per-poll delivery accounting, optional local tee."""
from __future__ import annotations

import gzip
import json
import logging
import threading
from typing import Any, Dict, Optional

from .config import Config
from .envelope import encode

log = logging.getLogger(__name__)


class DeliveryBatch:
    """Counts delivery results for the messages of one poll."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.sent = 0
        self.delivered = 0
        self.failed = 0
        self.last_error: Optional[str] = None

    def callback(self, err: Any, msg: Any) -> None:
        with self._lock:
            if err is not None:
                self.failed += 1
                self.last_error = str(err)
            else:
                self.delivered += 1


def build_kafka_producer(cfg: Config):
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": cfg.bootstrap_servers,
        "client.id": "citibike-producer",
        "enable.idempotence": True,      # no duplicates or reordering on broker retries
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,
        "batch.num.messages": 5000,
        "delivery.timeout.ms": 120000,   # librdkafka retries internally within this window
        "socket.keepalive.enable": True,
    })


class KafkaSink:
    def __init__(self, cfg: Config, producer: Any = None):
        self._p = producer if producer is not None else build_kafka_producer(cfg)
        self._tee_lock = threading.Lock()
        self._tee = gzip.open(cfg.tee_path, "at", encoding="utf-8") if cfg.tee_path else None

    def send(self, topic: str, key: str, envelope: Dict[str, Any], batch: Optional[DeliveryBatch] = None,
             timestamp_ms: Optional[int] = None) -> None:
        value = encode(envelope)
        kwargs: Dict[str, Any] = {"key": key.encode("utf-8"), "value": value}
        if batch is not None:
            kwargs["on_delivery"] = batch.callback
            batch.sent += 1
        if timestamp_ms is not None:
            kwargs["timestamp"] = timestamp_ms
        while True:
            try:
                self._p.produce(topic, **kwargs)
                break
            except BufferError:
                self._p.poll(0.5)   # local queue full: serve callbacks, then retry
        if self._tee is not None:
            with self._tee_lock:
                self._tee.write(value.decode("utf-8") + "\n")

    def poll(self, timeout: float = 0.0) -> None:
        self._p.poll(timeout)

    def flush(self, timeout: float = 15.0) -> int:
        """Returns the number of messages still undelivered after the timeout."""
        return int(self._p.flush(timeout))

    def close(self) -> None:
        remaining = self.flush(30.0)
        if remaining:
            log.error("%d messages undelivered at shutdown", remaining)
        if self._tee is not None:
            with self._tee_lock:
                self._tee.close()
