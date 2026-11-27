"""A lake table: hour-partitioned Kafka-sourced rows, written per batch, compacted per hour.

A lake table hides its key layout (prefix/dt=YYYY-MM-DD/hr=HH/<batch_id>.parquet, merged into
compacted.parquet by compaction) and the rule that keeps reads lossless while compaction runs:
compaction writes compacted.parquet before deleting the files it merged, and a reader that finds a
listed file gone re-reads the whole hour."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Mapping, MutableMapping, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from .compact_hour import CompactHour
from .hour_partitions import floor_hour, hour_from_key, hours_between, part_key, partition_path
from .schemas import TS, to_table
from .storage import Storage
from .storage_ops import put_parquet, read_parquet

log = logging.getLogger(__name__)


IDENTITY = ["kafka_topic", "kafka_partition", "kafka_offset"]


_EVENT_TIME = "fetched_at"                                     # every lake table's event time


_SORT_BY = (_EVENT_TIME, "kafka_partition", "kafka_offset")    # row order inside a compacted file


_COMPACTED = "compacted.parquet"


def _dedupe_identity(table: pa.Table) -> pa.Table:
    """Drop rows repeating a Kafka identity (a compacted file next to not yet deleted sources, or a
    batch rewritten after its hour was compacted)."""
    if table.num_rows == 0:
        return table
    ids = zip(*(table[c].to_pylist() for c in IDENTITY))
    seen = set()
    keep = []
    for i, k in enumerate(ids):
        if k not in seen:
            seen.add(k)
            keep.append(i)
    return table if len(keep) == table.num_rows else table.take(pa.array(keep))


@dataclass(frozen=True)
class LakeTable:
    """Kafka-sourced rows split into hour partitions by event time (fetched_at); no two rows share a
    Kafka identity."""

    prefix: str
    schema: pa.Schema

    # -- write --

    def write(self, storage: Storage, batch_id: str, rows: Sequence[Mapping[str, object]]) -> List[datetime]:
        """Write a batch's rows, one file per event-time hour, and return those hours.

        Overwrite-safe: a retry of the same batch rewrites the same files. If compaction has merged the
        batch's earlier files meanwhile, reads and the next compaction drop the duplicate rows."""
        groups: MutableMapping[datetime, List[Mapping[str, object]]] = {}
        for r in rows:
            groups.setdefault(floor_hour(r[_EVENT_TIME]), []).append(r)
        for hour, rs in sorted(groups.items()):
            put_parquet(storage, self._batch_key(hour, batch_id), to_table(rs, self.schema))
        return sorted(groups)

    # -- read --

    def read_window(self, storage: Storage, start: datetime, end: datetime,
                    columns: Optional[List[str]] = None, exclude_batch: Optional[str] = None) -> pa.Table:
        """Rows with start <= event time < end, each Kafka identity once, with only `columns` (default all).

        Rows written by `exclude_batch` are left out, so a retry never reads its own earlier output.
        Safe to run while compaction merges the same hours."""
        columns = self._columns(columns)
        needed = list(dict.fromkeys(columns + IDENTITY + [_EVENT_TIME]
                                    + (["batch_id"] if exclude_batch else [])))
        tables: List[pa.Table] = []
        for hour in hours_between(start, end):
            tables.extend(self._read_hour(storage, hour, needed, exclude_batch))
        if not tables:
            return self._empty(columns)
        table = _dedupe_identity(pa.concat_tables(tables))
        t = table[_EVENT_TIME]
        mask = pc.and_(pc.greater_equal(t, pa.scalar(start, TS)), pc.less(t, pa.scalar(end, TS)))
        if exclude_batch:
            ids = table["batch_id"]          # null-safe: a row without a batch_id is never excluded
            mask = pc.and_(mask, pc.or_kleene(pc.is_null(ids), pc.not_equal(ids, exclude_batch)))
        return table.filter(mask).select(columns)

    def read_batch(self, storage: Storage, batch_id: str, hours: Iterable[datetime],
                   columns: Optional[List[str]] = None) -> pa.Table:
        """The rows `batch_id` wrote in `hours` (as returned by `write`), also after compaction merged them."""
        columns = self._columns(columns)
        tables = []
        for hour in sorted(set(floor_hour(h) for h in hours)):
            try:
                tables.append(read_parquet(storage, self._batch_key(hour, batch_id), columns=columns))
            except FileNotFoundError:
                merged = read_parquet(storage, self._key(hour, _COMPACTED),
                                      columns=list(dict.fromkeys(columns + ["batch_id"])))
                tables.append(merged.filter(pc.equal(merged["batch_id"], batch_id)).select(columns))
        if not tables:
            return self._empty(columns)
        return pa.concat_tables(tables)

    # -- compact --

    def compact_hour(self, storage: Storage, hour: datetime) -> CompactHour:
        """Merge an hour's batch files into compacted.parquet: idempotent, and verified before deleting."""
        keys = self._list_hour(storage, hour)
        target = self._key(hour, _COMPACTED)
        sources = [k for k in keys if k != target]
        if not sources:
            return CompactHour(sources=0, rows=0)
        table = _dedupe_identity(pa.concat_tables([read_parquet(storage, k) for k in keys]))
        table = table.sort_by([(c, "ascending") for c in _SORT_BY])
        put_parquet(storage, target, table)          # written before anything is deleted (see _read_hour)
        written = read_parquet(storage, target, columns=IDENTITY).num_rows
        if written != table.num_rows:
            raise RuntimeError("compaction verification failed for %s: wrote %d of %d rows"
                               % (target, written, table.num_rows))
        for k in sources:
            storage.delete(k)
        return CompactHour(sources=len(sources), rows=table.num_rows)

    def hours(self, storage: Storage) -> List[datetime]:
        """Every hour partition holding at least one file."""
        found = {hour_from_key(key[len(self.prefix):]) for key in storage.list(self.prefix + "/")}
        return sorted(h for h in found if h is not None)

    # -- internals --

    def _columns(self, columns: Optional[List[str]]) -> List[str]:
        return list(columns) if columns is not None else list(self.schema.names)

    def _empty(self, columns: List[str]) -> pa.Table:
        return pa.Table.from_pylist([], schema=pa.schema([self.schema.field(c) for c in columns]))

    def _key(self, hour: datetime, name: str) -> str:
        return part_key(self.prefix, hour, name)

    def _batch_key(self, hour: datetime, batch_id: str) -> str:
        return self._key(hour, "%s.parquet" % batch_id)

    def _list_hour(self, storage: Storage, hour: datetime) -> List[str]:
        return [k for k in storage.list(partition_path(self.prefix, hour) + "/") if k.endswith(".parquet")]

    def _read_hour(self, storage: Storage, hour: datetime, columns: List[str],
                   exclude_batch: Optional[str]) -> List[pa.Table]:
        """Every file of one hour partition, consistent with a concurrent compaction.

        compact_hour writes compacted.parquet before deleting the files it merged. If a listed file
        vanishes, the hour was compacted mid-read: list it again and re-read the whole hour (the merged
        rows are now in compacted.parquet; overlaps are removed by _dedupe_identity). A file that
        vanishes twice is not compaction, so it raises instead of silently dropping rows."""
        skip = self._batch_key(hour, exclude_batch) if exclude_batch else None
        for attempt in (1, 2):
            try:
                return [read_parquet(storage, key, columns=columns)
                        for key in self._list_hour(storage, hour) if key != skip]
            except FileNotFoundError as exc:
                if attempt == 2:
                    raise
                log.info("%s changed while reading (%s), re-reading the hour after compaction",
                         partition_path(self.prefix, hour), exc)
        raise AssertionError("unreachable")
