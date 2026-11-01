"""Wiring for DAG tasks: config, storage and a Kafka reader built from the environment."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Tuple

from .config import PipelineConfig
from .kafka_io import KafkaBatchReader
from .storage import Storage, storage_from_config


def build() -> Tuple[PipelineConfig, Storage]:
    cfg = PipelineConfig.from_env()
    return cfg, storage_from_config(cfg)


def reader(cfg: PipelineConfig) -> KafkaBatchReader:
    return KafkaBatchReader(cfg.bootstrap_servers, cfg.consumer_group)


@contextmanager
def open_reader(cfg: PipelineConfig) -> Generator[KafkaBatchReader, None, None]:
    """A Kafka reader that is always closed, so a failed task does not leave a group member behind."""
    r = reader(cfg)
    try:
        yield r
    finally:
        r.close()
