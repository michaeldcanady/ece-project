"""Bounded Kafka reads. A batch reads exact offset ranges (assign + seek), never `subscribe`,
and offsets are committed only after outputs are written."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Mapping, MutableMapping, Optional, Set, Tuple

if TYPE_CHECKING:
    from confluent_kafka import Consumer

log = logging.getLogger(__name__)

# topic -> partition -> (start, end_exclusive)
Ranges = Mapping[str, Mapping[int, Tuple[int, int]]]
_MutableRanges = MutableMapping[str, MutableMapping[int, Tuple[int, int]]]


@dataclass
class Record:
    topic: str
    partition: int
    offset: int
    timestamp_ms: Optional[int]
    key: Optional[str]
    value: bytes


def ranges_to_json(ranges: Ranges) -> Mapping[str, Mapping[str, List[int]]]:
    return {t: {str(p): [s, e] for p, (s, e) in parts.items()} for t, parts in ranges.items()}


def ranges_from_json(obj: object) -> Ranges:
    """Parse `ranges_to_json`'s output. Raises ValueError when `obj` is not that shape."""
    if not isinstance(obj, Mapping):
        raise ValueError("ranges must be an object, got %r" % (obj,))
    ranges: _MutableRanges = {}
    for topic, parts in obj.items():
        if not isinstance(parts, Mapping):
            raise ValueError("ranges of %r must be an object, got %r" % (topic, parts))
        ranges[str(topic)] = {}
        for pid, bounds in parts.items():
            if not (isinstance(bounds, (list, tuple)) and len(bounds) == 2
                    and isinstance(bounds[0], int) and isinstance(bounds[1], int)):
                raise ValueError("range of %s/%s must be [start, end], got %r" % (topic, pid, bounds))
            ranges[str(topic)][int(str(pid))] = (bounds[0], bounds[1])
    return ranges


def total_messages(ranges: Ranges) -> int:
    return sum(e - s for parts in ranges.values() for (s, e) in parts.values())


def cap_ranges(positions: Ranges, max_per_partition: Optional[int] = None) -> Ranges:
    """The non-empty (start, end) ranges of `positions`, at most `max_per_partition` long."""
    ranges: _MutableRanges = {}
    for topic, parts in positions.items():
        for pid, (start, high) in parts.items():
            end = high if max_per_partition is None else min(high, start + max_per_partition)
            if end > start:
                ranges.setdefault(topic, {})[pid] = (start, end)
    return ranges


def fits_within(ranges: Ranges, positions: Ranges) -> bool:
    """True if every range starts where `positions` starts and ends at or below its high watermark."""
    for topic, parts in ranges.items():
        for pid, (start, end) in parts.items():
            pos = positions.get(topic, {}).get(pid)
            if pos is None or pos[0] != start or end > pos[1]:
                return False
    return True


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

        positions: _MutableRanges = {}
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
