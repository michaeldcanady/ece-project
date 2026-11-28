"""Hourly dimension build from the compacted station_information topic (full snapshot each run)."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

from citibike_contract import InvalidEnvelope, decode

from .config import TOPIC_INFO, PipelineConfig
from .dim_station_build import DimStationBuild
from .dim_station_row import DimStationRow
from .kafka_batch_reader import KafkaBatchReader
from .ranges import ranges_to_json, total_messages
from .station_information import StationInformation
from .station_information_table import StationInformationTable
from .storage import Storage
from .timeutil import utcnow

log = logging.getLogger(__name__)


def build_dim_station(reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
                      now: Optional[datetime] = None) -> DimStationBuild:
    now = now or utcnow()
    ranges = reader.plan([TOPIC_INFO], from_committed=False)      # whole topic: low -> high watermark
    records = reader.read(ranges, cfg.read_timeout_s) if ranges else []

    latest = {}   # station id -> its newest row
    rejected = 0
    for rec in records:                                           # offset order: last write per key wins
        try:
            env = decode(rec.value)
        except InvalidEnvelope:
            rejected += 1
            continue
        info = StationInformation.parse(env.payload) if env.event_type == "station_information" else None
        if not isinstance(info, StationInformation):
            rejected += 1
            continue
        latest[info.station_id] = DimStationRow.from_delivered(info, env, rec, now)
    if not latest:
        # never replace a good dimension with an empty one
        raise RuntimeError("no valid station_information records (read %d, rejected %d)" % (len(records), rejected))

    StationInformationTable().replace(storage, list(latest.values()), now)
    log.info("dim_station: %d stations from %d records (%d rejected)", len(latest), len(records), rejected)
    return DimStationBuild(stations=len(latest), records_read=len(records), rejected=rejected,
                           messages_planned=total_messages(ranges), ranges=json.dumps(ranges_to_json(ranges)))
