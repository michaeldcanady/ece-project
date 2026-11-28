"""The lake: lake tables (hour-partitioned Kafka-sourced rows, written per batch, compacted per hour)
and the fixed keys of the batch logs.

A lake table hides its key layout (prefix/dt=YYYY-MM-DD/hr=HH/<batch_id>.parquet, merged into
compacted.parquet by compaction) and the rule that keeps reads lossless while compaction runs:
compaction writes compacted.parquet before deleting the files it merged, and a reader that finds a
listed file gone re-reads the whole hour.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Mapping, MutableMapping, Optional

from .compact_hour import CompactHour
from .hour_partitions import partition_path
from .lake_table import LakeTable
from .schemas import RAW, STATUS, WEATHER
from .storage import Storage
from .timeutil import utcnow

log = logging.getLogger(__name__)

DLQ = "dlq"
GOLD_WINDOW = "gold/station_window_15m"
GOLD_AVAILABILITY = "gold/system_availability_5m"


# ---- lake tables ----------------------------------------------------------------------------

RAW_STATUS = LakeTable("raw/station_status", RAW)
RAW_WEATHER = LakeTable("raw/weather", RAW)
RAW_POLL_LOG = LakeTable("raw/poll_log", RAW)
CURATED_STATUS = LakeTable("curated/station_status", STATUS)
CURATED_WEATHER = LakeTable("curated/weather", WEATHER)

LAKE_TABLES = (RAW_STATUS, RAW_WEATHER, RAW_POLL_LOG, CURATED_STATUS, CURATED_WEATHER)
RAW_BY_EVENT_TYPE = {"station_status": RAW_STATUS, "weather_current": RAW_WEATHER, "poll_log": RAW_POLL_LOG}


def compact_due(storage: Storage, now: Optional[datetime] = None, lag_hours: int = 2) -> Mapping[str, CompactHour]:
    """Compact every lake table's hours that ended at least `lag_hours` ago.

    The lag keeps compaction away from the hours the micro-batch is still writing; it is not what makes
    concurrent reads safe. Reads stay lossless on their own (see LakeTable._read_hour), because
    lookbacks (3 h for weather), catch-up and replay all reach hours older than the lag."""
    now = now or utcnow()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)      # hour partitions are UTC
    cutoff = now - timedelta(hours=lag_hours)
    result: MutableMapping[str, CompactHour] = {}
    # TODO: add compact_due method to LakeTable, so that the DAG can call it directly instead of
    # iterating over LAKE_TABLES
    for table in LAKE_TABLES:
        for hour in table.hours(storage):
            if hour + timedelta(hours=1) > cutoff:
                continue
            res = table.compact_hour(storage, hour)
            if res.sources:
                name = partition_path(table.prefix, hour)
                result[name] = res
                log.info("compacted %s: %d files -> %d rows", name, res.sources, res.rows)
    return result
