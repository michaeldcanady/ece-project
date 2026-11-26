"""Reprocess the real messages Kafka still holds into an isolated lake.

Own lake directory and own consumer group: the live lake is never written and the live group's offsets never move.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import Mapping, Tuple

from citibike_pipeline import batch
from citibike_pipeline.config import DATA_TOPICS, PipelineConfig
from citibike_pipeline.kafka_io import KafkaBatchReader, Ranges
from citibike_pipeline.station_info import build_dim_station
from citibike_pipeline.storage import LocalStorage


@dataclass(frozen=True)
class Reprocessed:
    storage: LocalStorage
    cfg: PipelineConfig
    batches: int
    messages: int
    targets: Ranges        # (low, high) per partition when the run began: what "caught up" means


def reprocess(root: str, max_batches: int) -> Reprocessed:
    group = "citibike-e2e-%d" % int(time.time())
    cfg = replace(PipelineConfig.from_env(), local_root=root, storage="local", consumer_group=group,
                  read_timeout_s=120.0)
    storage = LocalStorage(root)
    reader = KafkaBatchReader(cfg.bootstrap_servers, group)
    try:
        targets = reader.positions(DATA_TOPICS, from_committed=False)
        build_dim_station(reader, storage, cfg)
        batches = messages = 0
        covered = {}  # (topic, partition) -> end offset read so far
        for i in range(max_batches):
            plan, stats = batch.run_batch("e2e_%d" % i, reader, storage, cfg)
            if stats is None:
                break
            batches += 1
            messages += plan.total_messages
            for topic, parts in plan.ranges.items():
                for pid, (_start, end) in parts.items():
                    covered[(topic, pid)] = end
            if _caught_up(covered, targets):
                break
        return Reprocessed(storage, cfg, batches, messages, targets)
    finally:
        reader.close()


def _caught_up(covered: Mapping[Tuple[str, int], int], targets: Ranges) -> bool:
    """True once every partition that held messages at the start has been read up to its start-time high mark."""
    return all(covered.get((topic, pid), low) >= high
               for topic, parts in targets.items() for pid, (low, high) in parts.items())
