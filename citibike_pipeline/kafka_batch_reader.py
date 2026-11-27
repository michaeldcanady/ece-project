"""Bounded Kafka reads. A batch reads exact offset ranges (assign + seek), never `subscribe`,
and offsets are committed only after outputs are written."""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

if TYPE_CHECKING:
    from confluent_kafka import Consumer
from .ranges import MutableRanges, Ranges, cap_ranges
from .record import Record

log = logging.getLogger(__name__)


class KafkaBatchReader:
    def __init__(self, bootstrap_servers: str, group_id: str, consumer: Optional[Consumer] = None):
        if consumer is None:
            from confluent_kafka import Consumer
            consumer = Consumer({
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            })
        self._c = consumer

    def plan(self, topics: List[str], max_per_partition: Optional[int] = None,
             from_committed: bool = True) -> Ranges:
        """Next range per partition: committed (or low watermark) up to the high watermark."""
        return cap_ranges(self.positions(topics, from_committed), max_per_partition)

    def positions(self, topics: List[str], from_committed: bool = True) -> Ranges:
        """(start, high watermark) for every partition, including empty ones. The start is the committed
        offset, or the low watermark when nothing is committed or retention has passed the commit."""
        from confluent_kafka import TopicPartition

        positions: MutableRanges = {}
        for topic in topics:
            md = self._c.list_topics(topic, timeout=10)
            tmeta = md.topics.get(topic)
            if tmeta is None or getattr(tmeta, "error", None) is not None or not tmeta.partitions:
                raise RuntimeError("topic %s is missing or unavailable (was create-topics run?)" % topic)
            for pid in sorted(tmeta.partitions):
                tp = TopicPartition(topic, pid)
                low, high = self._c.get_watermark_offsets(tp, timeout=10)
                start = low
                if from_committed:
                    committed = self._c.committed([tp], timeout=10)[0].offset
                    if committed is not None and committed >= 0:
                        start = max(committed, low)   # retention may have advanced past the commit
                positions.setdefault(topic, {})[pid] = (start, high)
        return positions

    def read(self, ranges: Ranges, timeout_s: float) -> List[Record]:
        """Read exactly the planned ranges. Raises on timeout rather than returning a short batch."""
        from confluent_kafka import KafkaError, TopicPartition

        pending = {(t, p): se for t, parts in ranges.items() for p, se in parts.items()}
        if not pending:
            return []
        self._c.assign([TopicPartition(t, p, s) for (t, p), (s, _e) in pending.items()])
        done: Set[Tuple[str, int]] = set()
        out: List[Record] = []
        deadline = time.monotonic() + timeout_s
        while len(done) < len(pending):
            if time.monotonic() > deadline:
                raise TimeoutError("kafka read timed out; %d of %d partitions incomplete"
                                   % (len(pending) - len(done), len(pending)))
            msg = self._c.poll(1.0)
            if msg is None:
                continue
            error = msg.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError("kafka error: %s" % error)
            topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
            if topic is None or partition is None or offset is None:
                raise RuntimeError("kafka message without a position")
            tp = (topic, partition)
            if tp not in pending or tp in done:
                continue
            start, end = pending[tp]
            if offset >= end:
                done.add(tp)
                continue
            ts_type, ts = msg.timestamp()
            raw_key = msg.key()
            key = raw_key.decode("utf-8", "replace") if isinstance(raw_key, bytes) and raw_key else None
            out.append(Record(topic, partition, offset, ts if ts_type != 0 and ts >= 0 else None, key,
                              msg.value() or b""))
            if offset >= end - 1:
                done.add(tp)
        self._c.unassign()
        out.sort(key=lambda r: (r.topic, r.partition, r.offset))
        log.info("read %d records from %d partitions", len(out), len(pending))
        return out

    def commit(self, ranges: Ranges) -> None:
        from confluent_kafka import TopicPartition

        offsets = [TopicPartition(t, p, e) for t, parts in ranges.items() for p, (_s, e) in parts.items()]
        if offsets:
            self._c.commit(offsets=offsets, asynchronous=False)

    def close(self) -> None:
        self._c.close()
