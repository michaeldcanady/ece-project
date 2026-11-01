"""Broker checks for the dev shell (run through mise, see `mise tasks`):

    python -m citibike_producer.broker check                      # broker reachable, every topic exists
    python -m citibike_producer.broker peek <topic> [--count N] [--newest]

`peek` reads without a consumer group commit, so it never moves the pipeline's offsets.
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Any, List, Optional, Tuple

from .config import Config
from .create_topics import topic_specs

# Topic names come from the same specs citibike_setup creates, so the check cannot drift from setup.
REQUIRED_TOPICS = [name for name, _partitions, _config in topic_specs(retention_days=1)]

# (partition, offset, key, value)
Message = Tuple[int, int, Optional[str], str]


def build_consumer(cfg: Config) -> Any:
    from confluent_kafka import Consumer

    return Consumer({
        "bootstrap.servers": cfg.bootstrap_servers,
        "group.id": "citibike-broker-peek",
        "enable.auto.commit": False,
    })


def missing_topics(consumer: Any, timeout_s: float = 10.0) -> List[str]:
    """Topics citibike_setup should have created but the broker does not have.
    Raises (KafkaException) if the broker is unreachable."""
    existing = consumer.list_topics(timeout=timeout_s).topics
    return [t for t in REQUIRED_TOPICS if t not in existing]


def peek(consumer: Any, topic: str, count: int, newest: bool = False, timeout_s: float = 10.0) -> List[Message]:
    """Up to `count` messages from each partition: the oldest, or the newest with `newest`."""
    from confluent_kafka import TopicPartition

    meta = consumer.list_topics(topic, timeout=timeout_s).topics.get(topic)
    if meta is None or getattr(meta, "error", None) is not None or not meta.partitions:
        raise RuntimeError("topic %s does not exist (trigger: mise run dag:trigger citibike_setup)" % topic)

    ends = {}
    starts = []
    for pid in sorted(meta.partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=timeout_s)
        start = max(low, high - count) if newest else low
        end = min(high, start + count)
        if end > start:
            ends[pid] = end
            starts.append(TopicPartition(topic, pid, start))
    if not starts:
        return []

    consumer.assign(starts)
    out: List[Message] = []
    pending = set(ends)
    deadline = time.monotonic() + timeout_s
    try:
        while pending and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error() or msg.partition() not in pending:
                continue
            if msg.offset() < ends[msg.partition()]:
                key = msg.key().decode("utf-8", "replace") if msg.key() else None
                out.append((msg.partition(), msg.offset(), key, (msg.value() or b"").decode("utf-8", "replace")))
            if msg.offset() >= ends[msg.partition()] - 1:      # compacted topics can skip offsets
                pending.discard(msg.partition())
    finally:
        consumer.unassign()
    if pending:
        raise TimeoutError("timed out reading %s partitions %s" % (topic, sorted(pending)))
    return sorted(out)


def main(argv: Optional[List[str]] = None, consumer: Any = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m citibike_producer.broker", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="fail unless the broker is reachable and every pipeline topic exists")
    p = sub.add_parser("peek", help="print messages from each partition of a topic")
    p.add_argument("topic")
    p.add_argument("--count", type=int, default=3, help="messages per partition (default 3)")
    p.add_argument("--newest", action="store_true", help="the newest messages instead of the oldest")
    args = ap.parse_args(argv)

    cfg = Config.from_env()
    consumer = consumer if consumer is not None else build_consumer(cfg)
    try:
        if args.cmd == "check":
            print("bootstrap: %s" % cfg.bootstrap_servers)
            try:
                missing = missing_topics(consumer)
            except Exception as exc:   # noqa: BLE001 - KafkaException; confluent_kafka is imported lazily
                print("UNREACHABLE: %s (is the kafka service up?)" % exc)
                return 1
            if missing:
                print("MISSING topics: %s (trigger: mise run dag:trigger citibike_setup)" % ", ".join(missing))
                return 1
            print("ok: all %d topics exist" % len(REQUIRED_TOPICS))
            return 0
        for partition, offset, key, value in peek(consumer, args.topic, args.count, args.newest):
            print("partition=%d offset=%d key=%s %s" % (partition, offset, key, value))
        return 0
    finally:
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
