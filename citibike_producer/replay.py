"""Replay mode: stream a captured sample (gzip or plain JSONL of envelopes) through the same topics."""
from __future__ import annotations

import gzip
import json
import logging
import threading
from typing import Any, Optional, TextIO

from .config import Config, TOPIC_BY_EVENT_TYPE
from .envelope import iso_to_epoch_ms, utc_now_iso
from .sink import KafkaSink

log = logging.getLogger(__name__)


def _open(path: str) -> TextIO:
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def _key_for(env: dict[str, Any], cfg: Config) -> str:
    et = env.get("event_type")
    payload = env.get("payload") or {}
    if et in ("station_status", "station_information"):
        sid = payload.get("station_id")
        return str(sid) if sid is not None else "_missing_station_id"
    if et == "weather_current":
        return str(payload.get("location_id") or cfg.weather_location_id)
    if et == "poll_log":
        return str(payload.get("job") or "poll_log")
    return "unknown"


def replay(path: str, sink: KafkaSink, cfg: Config, stop: threading.Event, speed: float = 1.0,
           max_gap_s: float = 5.0, loop: bool = False) -> int:
    """Republish captured envelopes in order.

    speed: 1 = original pacing, 60 = 60x faster, 0 = as fast as possible.
    Original fetched_at is preserved (event time). replayed_at and replay=true are added so
    latency can be measured against replay time. The Kafka record timestamp is left to the
    broker (now): old CreateTime values would make time-based retention delete replayed
    segments almost immediately.
    """
    sent = 0
    while not stop.is_set():
        prev_t: Optional[float] = None
        with _open(path) as fh:
            for line in fh:
                if stop.is_set():
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    env = json.loads(line)
                    topic = TOPIC_BY_EVENT_TYPE[env["event_type"]]
                    t = iso_to_epoch_ms(env["fetched_at"]) / 1000.0
                except (ValueError, KeyError):
                    log.warning("replay: skipping unreadable line")
                    continue
                if speed > 0 and prev_t is not None and t > prev_t:
                    stop.wait(min((t - prev_t) / speed, max_gap_s))
                prev_t = t
                env["replay"] = True
                env["replayed_at"] = utc_now_iso()
                sink.send(topic, _key_for(env, cfg), env)
                sent += 1
                if sent % 1000 == 0:
                    sink.poll(0)
        sink.flush(30.0)
        log.info("replay pass complete: %d messages so far", sent)
        if not loop:
            break
    return sent
