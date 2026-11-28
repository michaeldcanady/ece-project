"""Entry points for the ingest DAGs. Airflow-free: each function does one unit of work and returns
plain values, so the DAGs stay thin and everything here is testable without a scheduler.

A poll's carry-over state (feed URLs from discovery, the last feed timestamp for skip-unchanged,
the failure count) goes in and out as a PollState; the DAG keeps its JSON form in XCom between runs.
"""
from __future__ import annotations

import dataclasses
import os
import threading
from typing import Optional

from citibike_contract import TOPIC_INFO, TOPIC_STATUS

from .config import Config
from .feed_registry import FeedRegistry
from .gbfs_feed import GbfsFeed
from .http_client import HttpClient
from .kafka_sink import KafkaSink
from .poll import poll
from .poll_source import PollSource
from .poll_state import PollState
from .producer_like import ProducerLike
from .replay import replay
from .task import PollResult
from .weather_feed import WeatherFeed

JOBS = ("station_status", "station_information", "weather")


def _build_source(name: str, cfg: Config, client: HttpClient, registry: FeedRegistry) -> PollSource:
    if name == "station_status":
        return GbfsFeed(cfg, client, registry, "station_status", "station_status", TOPIC_STATUS)
    if name == "station_information":
        return GbfsFeed(cfg, client, registry, "station_information", "station_information", TOPIC_INFO)
    if name == "weather":
        return WeatherFeed(cfg, client)
    raise ValueError("unknown job %r (expected one of %s)" % (name, ", ".join(JOBS)))


def _job_config(cfg: Config, job: str) -> Config:
    if not cfg.capture_dir:
        return cfg
    os.makedirs(cfg.capture_dir, exist_ok=True)
    return dataclasses.replace(cfg, tee_path=os.path.join(cfg.capture_dir, "%s.jsonl.gz" % job))


def poll_once(job: str, state: Optional[PollState] = None, cfg: Optional[Config] = None,
              producer: Optional[ProducerLike] = None, client: Optional[HttpClient] = None) -> PollResult:
    """Poll one source once and publish to Kafka. Returns a PollResult instance.

    A failed poll is not an exception: it is already recorded in pipeline.poll_log. The caller
    decides whether to fail the task (after saving the returned state).
    """
    cfg = _job_config(cfg or Config.from_env(), job)
    state = state or PollState()
    sink = KafkaSink(cfg, producer=producer)
    try:
        client = client or HttpClient(cfg)
        registry = FeedRegistry(cfg, client)
        registry.restore(state.discovery)
        result = poll(_build_source(job, cfg, client, registry), state.job, cfg, sink)
    finally:
        # Short: poll already flushed for 15 s. Worst case per poll ~40 s, inside the DAG's 55 s
        # execution_timeout, so the state is always handed back before Airflow could kill the task.
        sink.close(timeout=10.0)
    new_state = PollState(job=result.state, discovery=registry.state() if job != "weather" else None)
    return PollResult(ok=result.ok, outcome=result.outcome, error=result.error, state=new_state)


def replay_capture(path: str, speed: float = 60.0, max_gap_s: float = 5.0, cfg: Optional[Config] = None,
                   producer: Optional[ProducerLike] = None) -> int:
    """Replay a capture file, or a capture directory, into Kafka. Returns the number of messages sent."""
    cfg = cfg or Config.from_env()
    sink = KafkaSink(cfg, producer=producer)
    try:
        return replay(path, sink, cfg, threading.Event(), speed, max_gap_s)
    finally:
        sink.close()
