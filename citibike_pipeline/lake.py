"""Layout of the lake and helpers to read time windows across minute files and compacted files."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from .schemas import TS
from .storage import Storage, read_parquet
from .timeutil import partition_path

log = logging.getLogger(__name__)

CURATED_STATUS = "curated/station_status"
CURATED_WEATHER = "curated/weather"
DIM_STATION_CURRENT = "curated/dim_station/current.parquet"
DLQ = "dlq"
GOLD_WINDOW = "gold/station_window_15m"
GOLD_AVAILABILITY = "gold/system_availability_5m"
SERVING_CURRENT = "gold/serving/current_station_state.parquet"
SERVING_AT_RISK = "gold/serving/at_risk.parquet"
SERVING_SUMMARY = "gold/serving/system_summary.parquet"
SERVING_BATCH_LOG = "gold/serving/batch_log_recent.parquet"
BATCH_LOG = "ops/batch_log"
COMPACTED_NAME = "compacted.parquet"

IDENTITY = ["kafka_topic", "kafka_partition", "kafka_offset"]


def part_key(prefix: str, when: datetime, name: str) -> str:
    return "%s/%s" % (partition_path(prefix, when), name)


def dedupe_identity(table: pa.Table) -> pa.Table:
    """Drop rows repeating a Kafka coordinate (e.g. a compacted file next to not yet deleted sources)."""
    if table.num_rows == 0:
        return table
    ids = zip(table["kafka_topic"].to_pylist(), table["kafka_partition"].to_pylist(),
              table["kafka_offset"].to_pylist())
    seen = set()
    keep = []
    for i, k in enumerate(ids):
        if k not in seen:
            seen.add(k)
            keep.append(i)
    return table if len(keep) == table.num_rows else table.take(pa.array(keep))


def hours_between(start: datetime, end: datetime) -> Iterable[datetime]:
    cur = start.replace(minute=0, second=0, microsecond=0)
    while cur <= end:
        yield cur
        cur += timedelta(hours=1)


def _read_hour(storage: Storage, hour_prefix: str, columns: List[str],
               exclude_batch: Optional[str]) -> List[pa.Table]:
    """Every file of one hour partition, consistent with a concurrent compaction.

    Compaction writes compacted.parquet before deleting the minute files it merged. If a listed file
    vanishes, the hour was compacted mid-read: list it again and re-read the whole hour (the merged
    rows are now in compacted.parquet; overlaps are removed later by dedupe_identity). A file that
    vanishes twice is not compaction, so it raises instead of silently dropping rows.
    """
    for attempt in (1, 2):
        try:
            return [read_parquet(storage, key, columns=columns)
                    for key in storage.list(hour_prefix)
                    if key.endswith(".parquet")
                    and not (exclude_batch and key.endswith("/%s.parquet" % exclude_batch))]
        except FileNotFoundError as exc:
            if attempt == 2:
                raise
            log.info("%s changed while reading (%s), re-reading the hour after compaction", hour_prefix, exc)
    raise AssertionError("unreachable")


def read_batch_file(storage: Storage, key: str, batch_id: str, columns: List[str]) -> pa.Table:
    """The rows a batch wrote to `key`, also after compaction merged that file into compacted.parquet."""
    try:
        return read_parquet(storage, key, columns=columns)
    except FileNotFoundError:
        merged = key.rsplit("/", 1)[0] + "/" + COMPACTED_NAME
        table = read_parquet(storage, merged, columns=sorted(set(columns) | {"batch_id"}))
        return table.filter(pc.equal(table["batch_id"], batch_id)).select(columns)


def load_window(storage: Storage, prefix: str, start: datetime, end: datetime,
                columns: List[str], exclude_batch: Optional[str] = None,
                time_col: str = "fetched_at") -> pa.Table:
    """Rows with start <= time_col < end, across minute files and compacted files.

    `columns` must include the Kafka identity columns and `time_col`. Files written by
    `exclude_batch` are skipped so a retry never reads its own earlier output.
    Safe to run while compaction merges the same hours (see _read_hour).
    """
    tables: List[pa.Table] = []
    for hour in hours_between(start, end):
        tables.extend(_read_hour(storage, partition_path(prefix, hour) + "/", columns, exclude_batch))
    if not tables:
        return pa.Table.from_pylist([], schema=pa.schema([(c, pa.null()) for c in columns]))
    table = dedupe_identity(pa.concat_tables(tables))
    mask = pc.and_(pc.greater_equal(table[time_col], pa.scalar(start, TS)),
                   pc.less(table[time_col], pa.scalar(end, TS)))
    return table.filter(mask)
