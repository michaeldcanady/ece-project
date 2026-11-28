"""Replay mode: stream a captured sample (gzip or plain JSONL of envelopes) through the same topics."""
from __future__ import annotations

import glob
import gzip
import heapq
import logging
import os
import threading
from typing import Iterator, List, Optional, TextIO, Tuple

from citibike_contract import TOPIC_BY_EVENT_TYPE, Envelope, InvalidEnvelope, MissingKafkaKey, decode, kafka_key, utcnow

from .config import Config
from .kafka_sink import KafkaSink

log = logging.getLogger(__name__)


def _open(path: str) -> TextIO:
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def _envelopes(path: str) -> Iterator[Tuple[float, Envelope]]:
    """(event time, envelope) for every valid envelope line of one capture file, in file order."""
    with _open(path) as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                env = decode(line.encode("utf-8"))
            except InvalidEnvelope as exc:
                log.warning("replay: skipping line %d of %s: %s", lineno, path, exc)
                continue
            yield env.fetched_at.timestamp(), env


def capture_files(path: str) -> List[str]:
    """A capture is one file, or a directory of per-job files (CB_CAPTURE_DIR)."""
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "*.jsonl.gz")) + glob.glob(os.path.join(path, "*.jsonl")))
        if not files:
            raise FileNotFoundError("no *.jsonl(.gz) capture files in %s" % path)
        return files
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return [path]


def replay(path: str, sink: KafkaSink, cfg: Config, stop: threading.Event, speed: float = 1.0,  # noqa: PLR0913
           max_gap_s: float = 5.0, loop: bool = False) -> int:
    """Republish captured envelopes in event-time order (files of a capture directory are merged).

    speed: 1 = original pacing, 60 = 60x faster, 0 = as fast as possible.
    Original fetched_at is preserved (event time). replayed_at and replay=true are added so
    latency can be measured against replay time. The Kafka record timestamp is left to the
    broker (now): old CreateTime values would make time-based retention delete replayed
    segments almost immediately.
    """
    files = capture_files(path)
    sent = 0
    while not stop.is_set():
        prev_t: Optional[float] = None
        stream = heapq.merge(*(_envelopes(f) for f in files), key=lambda te: te[0])
        for t, env in stream:
            if stop.is_set():
                break
            if speed > 0 and prev_t is not None and t > prev_t:
                stop.wait(min((t - prev_t) / speed, max_gap_s))
            prev_t = t
            replayed = env.mark_replayed(utcnow())
            try:
                key = kafka_key(replayed)
            except MissingKafkaKey as exc:
                log.warning("replay: skipping an envelope: %s", exc)
                continue
            sink.send(TOPIC_BY_EVENT_TYPE[replayed.event_type], key, replayed)
            sent += 1
            if sent % 1000 == 0:
                sink.poll(0)
        sink.flush(30.0)
        log.info("replay pass complete: %d messages so far", sent)
        if not loop:
            break
    return sent
