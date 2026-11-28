"""The serving files: what the dashboard reads, overwritten every batch.

This module owns their keys and the rules for writing them: history first, serving files last, and serving
time never moves backwards.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from . import lake
from .hour_partitions import part_key
from .schemas import CURRENT_STATE, SYSTEM_AVAILABILITY, WINDOW_15M, to_table
from .snapshot import Snapshot, SystemAvailability
from .storage import Storage
from .storage_ops import put_parquet, read_parquet

CURRENT = "gold/serving/current_station_state.parquet"
AT_RISK = "gold/serving/at_risk.parquet"
SUMMARY = "gold/serving/system_summary.parquet"


def read_summary(storage: Storage) -> Optional[SystemAvailability]:
    """The last published system summary, or None before the first publish."""
    if not storage.exists(SUMMARY):
        return None
    return SystemAvailability(**read_parquet(storage, SUMMARY).to_pylist()[0])


def serving_as_of(storage: Storage, batch_as_of: datetime) -> datetime:
    """The as-of time to publish at. A catch-up batch can end earlier than the previous one (partitions drain
    at different speeds), but serving time never moves backwards; a retry sees its own earlier value and
    stays stable."""
    prev = read_summary(storage)
    return prev.as_of if prev is not None and prev.as_of > batch_as_of else batch_as_of


def publish(storage: Storage, snap: Snapshot) -> None:
    """Write the snapshot's history, then the serving files, last of all."""
    put_parquet(storage, part_key(lake.GOLD_WINDOW, snap.as_of, "%s.parquet" % snap.batch_id),
                to_table(snap.windows, WINDOW_15M))
    bucket = snap.availability.bucket_start
    put_parquet(storage, part_key(lake.GOLD_AVAILABILITY, snap.as_of, "%s.parquet" % bucket.strftime("%H%M")),
                to_table([snap.availability], SYSTEM_AVAILABILITY))
    put_parquet(storage, AT_RISK, to_table(snap.at_risk, CURRENT_STATE))
    put_parquet(storage, SUMMARY, to_table([snap.availability], SYSTEM_AVAILABILITY))
    put_parquet(storage, CURRENT, to_table(snap.states, CURRENT_STATE))
