"""Create the pipeline's Kafka topics (idempotent: existing topics are left alone).
Called by the citibike_setup DAG."""
from __future__ import annotations

import logging
from typing import Any, Dict

from confluent_kafka.admin import AdminClient, NewTopic

from citibike_contract import TOPIC_DLQ, TOPIC_INFO, TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER

log = logging.getLogger(__name__)

DAY_MS = 24 * 3600 * 1000


def topic_specs(retention_days: int):
    retention = str(retention_days * DAY_MS)
    return [
        # name, partitions, config
        (TOPIC_STATUS, 3, {"retention.ms": retention, "compression.type": "producer"}),
        (TOPIC_INFO, 1, {"cleanup.policy": "compact", "segment.ms": str(DAY_MS),
                         "min.cleanable.dirty.ratio": "0.1"}),
        (TOPIC_WEATHER, 1, {"retention.ms": retention}),
        (TOPIC_POLL_LOG, 1, {"retention.ms": retention}),
        (TOPIC_DLQ, 1, {"retention.ms": retention}),
    ]


def ensure_topics(bootstrap_servers: str, replication: int = 1, retention_days: int = 21,
                  admin: Any = None) -> Dict[str, str]:
    """Create any missing topic. Returns topic -> "created" | "exists"; raises if any creation failed."""
    admin = admin or AdminClient({"bootstrap.servers": bootstrap_servers})
    new = [NewTopic(n, num_partitions=p, replication_factor=replication, config=c)
           for n, p, c in topic_specs(retention_days)]
    result: Dict[str, str] = {}
    failed: Dict[str, str] = {}
    for name, fut in admin.create_topics(new).items():
        try:
            fut.result()
            result[name] = "created"
        except Exception as exc:   # noqa: BLE001 - KafkaException carries the error code in its text
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                result[name] = "exists"
            else:
                failed[name] = str(exc)
    for name, status in sorted(result.items()):
        log.info("topic %s: %s", name, status)
    if failed:
        raise RuntimeError("could not create topics: %s" % failed)
    return result
