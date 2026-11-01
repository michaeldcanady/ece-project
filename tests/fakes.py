"""Minimal stand-ins for confluent_kafka objects used by KafkaBatchReader."""
import gzip
import json
import zlib
from types import SimpleNamespace

from citibike_contract import TOPIC_BY_EVENT_TYPE, TOPIC_INFO, TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER


class FakeMessage:
    def __init__(self, topic, partition, offset, key, value, ts):
        self._t, self._p, self._o, self._k, self._v, self._ts = topic, partition, offset, key, value, ts

    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def key(self): return self._k
    def value(self): return self._v
    def timestamp(self): return (1, self._ts)
    def error(self): return None


class FakeConsumer:
    """In-memory broker: log[(topic, partition)] = [(key, value, ts_ms)], plus group commits."""

    def __init__(self, partitions=None):
        self.log = {}
        self.commits = {}
        self.partitions = partitions or {}
        self._assigned = {}
        self.closed = False

    def append(self, topic, partition, key, value, ts=0):
        self.log.setdefault((topic, partition), []).append((key.encode(), value, ts))

    # --- confluent_kafka.Consumer subset
    def list_topics(self, topic=None, timeout=0):
        if topic is None:   # all topics, like the real client
            return SimpleNamespace(topics={t: SimpleNamespace(partitions={i: None for i in range(n)}, error=None)
                                           for t, n in self.partitions.items() if n})
        n = self.partitions.get(topic, 0)
        meta = SimpleNamespace(partitions={i: None for i in range(n)}, error=None)
        return SimpleNamespace(topics={topic: meta} if n else {})

    def get_watermark_offsets(self, tp, timeout=0):
        return 0, len(self.log.get((tp.topic, tp.partition), []))

    def committed(self, tps, timeout=0):
        return [SimpleNamespace(offset=self.commits.get((tp.topic, tp.partition), -1001)) for tp in tps]

    def assign(self, tps):
        self._assigned = {(tp.topic, tp.partition): tp.offset for tp in tps}

    def unassign(self):
        self._assigned = {}

    def poll(self, timeout=0):
        for tp, pos in self._assigned.items():
            entries = self.log.get(tp, [])
            if pos < len(entries):
                key, value, ts = entries[pos]
                self._assigned[tp] = pos + 1
                return FakeMessage(tp[0], tp[1], pos, key, value, ts)
        return None

    def commit(self, offsets=None, asynchronous=True):
        for tp in offsets:
            self.commits[(tp.topic, tp.partition)] = tp.offset

    def close(self):
        self.closed = True


def load_capture(path, partitions=3):
    """Spread a producer capture over a FakeConsumer the way the real topics are keyed."""
    from citibike_producer.config import Config
    from citibike_producer.replay import _key_for
    fc = FakeConsumer(partitions={TOPIC_STATUS: partitions, TOPIC_WEATHER: 1, TOPIC_POLL_LOG: 1, TOPIC_INFO: 1})
    cfg = Config()
    with gzip.open(path, "rt") as fh:
        for line in fh:
            env = json.loads(line)
            topic = TOPIC_BY_EVENT_TYPE[env["event_type"]]
            key = _key_for(env, cfg)
            part = zlib.crc32(key.encode()) % partitions if topic == TOPIC_STATUS else 0
            fc.append(topic, part, key, line.strip().encode(), ts=0)
    return fc
