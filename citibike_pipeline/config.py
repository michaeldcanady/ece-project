"""Pipeline configuration (env prefix CB_, shared with the producer)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from citibike_contract import TOPIC_INFO, TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER  # noqa: F401

# Topics the micro-batch consumes. station_info has its own hourly DAG.
DATA_TOPICS = [TOPIC_STATUS, TOPIC_WEATHER, TOPIC_POLL_LOG]


@dataclass(frozen=True)
class PipelineConfig:
    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = "citibike-micro-batch"

    # Storage: "local" (dev, a docker volume) or "s3" (AWS).
    storage: str = "local"
    local_root: str = "data"
    s3_bucket: str = ""
    s3_prefix: str = "citibike"
    s3_endpoint_url: Optional[str] = None   # set only for S3-compatible emulators

    # Micro-batch bounds
    max_batch_per_partition: int = 20000     # caps catch-up batch size after an outage
    read_timeout_s: float = 45.0

    # Metrics (all event-time, minutes unless noted)
    window_minutes: float = 15.0
    context_minutes: float = 30.0            # history read for flags and windows
    risk_horizon_minutes: float = 30.0
    min_coverage_minutes: float = 10.0
    noise_floor_bikes: int = 2
    stale_minutes: float = 30.0              # ASSUMPTION: tune from the observed age distribution
    future_skew_s: int = 300
    status_interval_s: float = 60.0          # must match the citibike_ingest_status schedule
    gap_threshold_s: int = 150               # 2.5 poll intervals

    @classmethod
    def from_env(cls) -> PipelineConfig:
        d = cls()
        g = os.environ.get
        return cls(
            bootstrap_servers=g("CB_BOOTSTRAP_SERVERS", d.bootstrap_servers),
            consumer_group=g("CB_CONSUMER_GROUP", d.consumer_group),
            storage=g("CB_STORAGE", d.storage),
            local_root=g("CB_LOCAL_ROOT", d.local_root),
            s3_bucket=g("CB_S3_BUCKET", d.s3_bucket),
            s3_prefix=g("CB_S3_PREFIX", d.s3_prefix),
            s3_endpoint_url=(g("CB_S3_ENDPOINT_URL") or None),
            max_batch_per_partition=int(g("CB_MAX_BATCH_PER_PARTITION", d.max_batch_per_partition)),
            read_timeout_s=float(g("CB_READ_TIMEOUT_S", d.read_timeout_s)),
            window_minutes=float(g("CB_WINDOW_MINUTES", d.window_minutes)),
            context_minutes=float(g("CB_CONTEXT_MINUTES", d.context_minutes)),
            risk_horizon_minutes=float(g("CB_RISK_HORIZON_MINUTES", d.risk_horizon_minutes)),
            min_coverage_minutes=float(g("CB_MIN_COVERAGE_MINUTES", d.min_coverage_minutes)),
            noise_floor_bikes=int(g("CB_NOISE_FLOOR_BIKES", d.noise_floor_bikes)),
            stale_minutes=float(g("CB_STALE_MINUTES", d.stale_minutes)),
            future_skew_s=int(g("CB_FUTURE_SKEW_S", d.future_skew_s)),
            status_interval_s=float(g("CB_STATUS_INTERVAL_S", d.status_interval_s)),
            gap_threshold_s=int(g("CB_GAP_THRESHOLD_S", d.gap_threshold_s)),
        )
