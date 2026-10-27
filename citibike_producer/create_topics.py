"""Create the pipeline's Kafka topics (idempotent: existing topics are left alone).

  CB_BOOTSTRAP_SERVERS=localhost:9092 uv run citibike-create-topics --replication 1
"""
from __future__ import annotations

import argparse
import os
import sys

from confluent_kafka.admin import AdminClient, NewTopic

DAY_MS = 24 * 3600 * 1000


def topic_specs(retention_days: int):
    retention = str(retention_days * DAY_MS)
    return [
        # name, partitions, config
        ("citibike.station_status", 3, {"retention.ms": retention, "compression.type": "producer"}),
        ("citibike.station_info", 1, {"cleanup.policy": "compact", "segment.ms": str(DAY_MS),
                                      "min.cleanable.dirty.ratio": "0.1"}),
        ("weather.current", 1, {"retention.ms": retention}),
        ("pipeline.poll_log", 1, {"retention.ms": retention}),
        ("pipeline.dlq", 1, {"retention.ms": retention}),
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--replication", type=int, default=1)
    ap.add_argument("--retention-days", type=int, default=21)
    args = ap.parse_args()

    admin = AdminClient({"bootstrap.servers": os.environ.get("CB_BOOTSTRAP_SERVERS", "localhost:9092")})
    new = [NewTopic(n, num_partitions=p, replication_factor=args.replication, config=c)
           for n, p, c in topic_specs(args.retention_days)]
    rc = 0
    for name, fut in admin.create_topics(new).items():
        try:
            fut.result()
            print("created  %s" % name)
        except Exception as exc:   # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print("exists   %s" % name)
            else:
                print("FAILED   %s: %s" % (name, exc))
                rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
