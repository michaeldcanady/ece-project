"""Merge the per-minute files of completed hours into one file per hour (idempotent, verified before delete)."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

from . import lake
from .storage import Storage, put_parquet, read_parquet
from .timeutil import utcnow

log = logging.getLogger(__name__)

# prefix -> columns to sort by
COMPACT_PREFIXES = {
    "raw/station_status": ["fetched_at", "kafka_partition", "kafka_offset"],
    "raw/weather": ["fetched_at", "kafka_partition", "kafka_offset"],
    "raw/poll_log": ["fetched_at", "kafka_partition", "kafka_offset"],
    lake.CURATED_STATUS: ["fetched_at", "kafka_partition", "kafka_offset"],
    lake.CURATED_WEATHER: ["fetched_at", "kafka_partition", "kafka_offset"],
}
_PART = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/hr=(\d{2})/")


def list_partitions(storage: Storage, prefix: str) -> List[Tuple[str, int]]:
    parts = set()
    for key in storage.list(prefix + "/"):
        m = _PART.search(key)
        if m:
            parts.add((m.group(1), int(m.group(2))))
    return sorted(parts)


def compact_partition(storage: Storage, prefix: str, dt: str, hr: int, sort_cols: List[str]) -> Dict[str, int]:
    base = "%s/dt=%s/hr=%02d/" % (prefix, dt, hr)
    keys = [k for k in storage.list(base) if k.endswith(".parquet")]
    target = base + lake.COMPACTED_NAME
    sources = [k for k in keys if k != target]
    if not sources:
        return {"sources": 0, "rows": 0}
    table = lake.dedupe_identity(pa.concat_tables([read_parquet(storage, k) for k in keys]))
    table = table.sort_by([(c, "ascending") for c in sort_cols])
    put_parquet(storage, target, table)
    written = read_parquet(storage, target, columns=lake.IDENTITY).num_rows   # verify before deleting anything
    if written != table.num_rows:
        raise RuntimeError("compaction verification failed for %s: wrote %d of %d rows"
                           % (target, written, table.num_rows))
    for k in sources:
        storage.delete(k)
    return {"sources": len(sources), "rows": table.num_rows}


def compact_due(storage: Storage, now: Optional[datetime] = None, lag_hours: int = 2) -> Dict[str, Dict[str, int]]:
    """Compact hours that ended at least `lag_hours` ago. The lag keeps compaction away from the hours
    the micro-batch is still writing; it is not what makes concurrent reads safe. Readers re-read an
    hour that changes under them (lake._read_hour, lake.read_batch_file), because lookbacks (3 h for
    weather), catch-up and replay all reach hours older than the lag."""
    now = now or utcnow()
    cutoff = now - timedelta(hours=lag_hours)
    result: Dict[str, Dict[str, int]] = {}
    for prefix, sort_cols in COMPACT_PREFIXES.items():
        for dt, hr in list_partitions(storage, prefix):
            hour_end = datetime.strptime(dt, "%Y-%m-%d").replace(hour=hr, tzinfo=now.tzinfo) + timedelta(hours=1)
            if hour_end > cutoff:
                continue
            res = compact_partition(storage, prefix, dt, hr, sort_cols)
            if res["sources"]:
                result["%s/dt=%s/hr=%02d" % (prefix, dt, hr)] = res
                log.info("compacted %s/dt=%s/hr=%02d: %d files -> %d rows", prefix, dt, hr, res["sources"], res["rows"])
    return result
