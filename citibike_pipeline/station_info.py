"""Hourly dimension build from the compacted station_information topic (full snapshot each run)."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from . import lake
from .config import TOPIC_INFO, PipelineConfig
from .kafka_io import KafkaBatchReader, ranges_to_json, total_messages
from .schemas import DIM_STATION, to_table
from .storage import Storage, put_parquet
from .timeutil import parse_iso, utcnow
from .validation import parse_envelope, validate_station_info_payload

log = logging.getLogger(__name__)


def build_dim_station(reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
                      now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or utcnow()
    ranges = reader.plan([TOPIC_INFO], from_committed=False)      # whole topic: low -> high watermark
    records = reader.read(ranges, cfg.read_timeout_s) if ranges else []

    latest: Dict[str, Dict[str, Any]] = {}
    rejected = 0
    for rec in records:                                           # offset order: last write per key wins
        env, errors = parse_envelope(rec.value)
        if errors or env["event_type"] != "station_information":
            rejected += 1
            continue
        p = env["payload"]
        if validate_station_info_payload(p):
            rejected += 1
            continue
        latest[p["station_id"]] = {
            "station_id": p["station_id"], "name": p.get("name"), "short_name": p.get("short_name"),
            "lat": float(p["lat"]), "lon": float(p["lon"]), "capacity": p.get("capacity"),
            "region_id": str(p["region_id"]) if p.get("region_id") is not None else None,
            "feed_last_updated": env.get("feed_last_updated"), "fetched_at": parse_iso(env["fetched_at"]),
            "kafka_partition": rec.partition, "kafka_offset": rec.offset, "snapshot_at": now,
        }
    if not latest:
        # never replace a good dimension with an empty one
        raise RuntimeError("no valid station_information records (read %d, rejected %d)" % (len(records), rejected))

    table = to_table(sorted(latest.values(), key=lambda r: r["station_id"]), DIM_STATION)
    snap_key = "curated/dim_station/snapshot_dt=%s/hr=%02d/dim_station.parquet" % (now.strftime("%Y-%m-%d"), now.hour)
    put_parquet(storage, snap_key, table)
    put_parquet(storage, lake.DIM_STATION_CURRENT, table)
    log.info("dim_station: %d stations from %d records (%d rejected)", len(latest), len(records), rejected)
    return {"stations": len(latest), "records_read": len(records), "rejected": rejected,
            "messages_planned": total_messages(ranges), "ranges": json.dumps(ranges_to_json(ranges))}
