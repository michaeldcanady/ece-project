"""Entry points for the ingest DAGs. Airflow-free: each function does one unit of work and returns
plain dicts, so the DAGs stay thin and everything here is testable without a scheduler.

A poll's carry-over state (feed URLs from discovery, the last feed timestamp for skip-unchanged,
the failure count) goes in and out as a JSON-able dict; the DAG keeps it in XCom between runs.
"""
from __future__ import annotations

import dataclasses
import os
import threading
from typing import Any, Dict, Optional

from citibike_contract import TOPIC_INFO, TOPIC_STATUS

from .config import Config
from .jobs import GbfsStationJob, Job, WeatherJob
from .replay import replay
from .sink import KafkaSink
from .sources import FeedRegistry, HttpClient

JOBS = ("station_status", "station_information", "weather")


def _build_job(name: str, cfg: Config, sink: KafkaSink, client: HttpClient,
               registry: FeedRegistry) -> Job:
    if name == "station_status":
        return GbfsStationJob(cfg, sink, client, registry, "station_status", "station_status", TOPIC_STATUS)
    if name == "station_information":
        return GbfsStationJob(cfg, sink, client, registry, "station_information", "station_information",
                              TOPIC_INFO)
    if name == "weather":
        return WeatherJob(cfg, sink, client)
    raise ValueError("unknown job %r (expected one of %s)" % (name, ", ".join(JOBS)))


def _job_config(cfg: Config, job: str) -> Config:
    if not cfg.capture_dir:
        return cfg
    os.makedirs(cfg.capture_dir, exist_ok=True)
    return dataclasses.replace(cfg, tee_path=os.path.join(cfg.capture_dir, "%s.jsonl.gz" % job))


def poll_once(job: str, state: Optional[Dict[str, Any]] = None, cfg: Optional[Config] = None,
              producer: Any = None, client: Optional[HttpClient] = None) -> Dict[str, Any]:
    """Poll one source once and publish to Kafka. Returns {"ok", "outcome", "error", "state"}.

    A failed poll is not an exception: it is already recorded in pipeline.poll_log. The caller
    decides whether to fail the task (after saving the returned state).
    """
    cfg = _job_config(cfg or Config.from_env(), job)
    state = state or {}
    sink = KafkaSink(cfg, producer=producer)
    try:
        client = client or HttpClient(cfg)
        registry = FeedRegistry(cfg, client)
        registry.restore(state.get("discovery"))
        j = _build_job(job, cfg, sink, client, registry)
        j.restore(state.get("job"))
        ok = j.run_once()
    finally:
        # Short: run_once already flushed for 15 s. Worst case per poll ~40 s, inside the DAG's 55 s
        # execution_timeout, so the state is always handed back before Airflow could kill the task.
        sink.close(timeout=10.0)
    new_state: Dict[str, Any] = {"job": j.state()}
    if job != "weather":
        new_state["discovery"] = registry.state()
    return {"ok": ok, "outcome": j.last_outcome, "error": j.last_error, "state": new_state}


def replay_capture(path: str, speed: float = 60.0, max_gap_s: float = 5.0, cfg: Optional[Config] = None,
                   producer: Any = None) -> int:
    """Replay a capture file, or a capture directory, into Kafka. Returns the number of messages sent."""
    cfg = cfg or Config.from_env()
    sink = KafkaSink(cfg, producer=producer)
    try:
        return replay(path, sink, cfg, threading.Event(), speed, max_gap_s)
    finally:
        sink.close()
