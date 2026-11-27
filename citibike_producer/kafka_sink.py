"""Kafka sink: idempotent producer, per-poll delivery accounting, optional local tee."""
from __future__ import annotations

import gzip
import logging
import threading
from typing import Optional

from citibike_contract import Envelope, encode

from .config import Config
from .delivery_batch import DeliveryBatch
from .producer_like import ProducerLike

log = logging.getLogger(__name__)


def build_kafka_producer(cfg: Config) -> ProducerLike:
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
    def __init__(self, cfg: Config, producer: Optional[ProducerLike] = None):
        self._p = producer if producer is not None else build_kafka_producer(cfg)
        self._tee_lock = threading.Lock()
        self._tee = gzip.open(cfg.tee_path, "at", encoding="utf-8") if cfg.tee_path else None

    def send(self, topic: str, key: str, envelope: Envelope, batch: Optional[DeliveryBatch] = None,
             timestamp_ms: Optional[int] = None) -> None:
        value = encode(envelope)
        on_delivery = batch.callback if batch is not None else None
        if batch is not None:
            batch.sent += 1
        while True:
            try:
                self._p.produce(topic, key=key.encode("utf-8"), value=value, on_delivery=on_delivery,
                                timestamp=timestamp_ms or 0)      # 0 lets the client stamp the send time
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

    def close(self, timeout: float = 30.0) -> None:
        remaining = self.flush(timeout)
        if remaining:
            log.error("%d messages undelivered at shutdown", remaining)
        if self._tee is not None:
            with self._tee_lock:
                self._tee.close()
