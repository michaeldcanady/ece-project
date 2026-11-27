"""What the day's curated station rows say about data health: staleness, repeats, ordering and empty stations."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import List, Mapping, Optional

import pyarrow as pa
import pyarrow.compute as pc
from pydantic import BaseModel

from . import lake
from .empty_station import EmptyStation
from .storage import Storage

AGE_EDGES_S = [60, 300, 900, 1800, 3600, 7200, 21600]     # for the stale-threshold distribution
STATUS_COLS = ["station_id", "fetched_at", "age_s", "is_stale", "is_repeat", "is_out_of_order",
               "is_unknown_station", "num_bikes_available", "is_installed", "is_renting", "is_returning"]


class SectionStatus(BaseModel):
    observations: int
    distinct_stations: int
    stale_share: Optional[float]
    repeat_share: Optional[float]
    out_of_order: int
    unknown_station_rows: int
    age_cdf: Mapping[str, float]
    top_empty_stations: List[EmptyStation]
    empty_share_by_hour_utc: Mapping[str, float]

    @staticmethod
    def read_day(storage: Storage, start: datetime) -> SectionStatus:
        """The section for the 24 hours from `start`, read one hour at a time to bound memory."""
        rows = stale = repeats = ooo = unknown = 0
        stations = set()
        age_le: Counter = Counter()                       # edge -> rows with age <= edge
        empty_by_station: Counter = Counter()
        obs_by_station: Counter = Counter()
        empty_by_hour: Counter = Counter()
        obs_by_hour: Counter = Counter()
        for h in range(24):
            hs = start + timedelta(hours=h)
            t = lake.CURATED_STATUS.read_window(storage, hs, hs + timedelta(hours=1), STATUS_COLS)
            if t.num_rows == 0:
                continue
            rows += t.num_rows
            stale += pc.sum(t["is_stale"].cast(pa.int64())).as_py() or 0
            repeats += pc.sum(t["is_repeat"].cast(pa.int64())).as_py() or 0
            ooo += pc.sum(t["is_out_of_order"].cast(pa.int64())).as_py() or 0
            unknown += pc.sum(t["is_unknown_station"].cast(pa.int64())).as_py() or 0
            stations.update(pc.unique(t["station_id"]).to_pylist())
            for e in AGE_EDGES_S:
                age_le[e] += pc.sum(pc.less_equal(t["age_s"], e).cast(pa.int64())).as_py() or 0
            operational = pc.and_(pc.and_(t["is_installed"], t["is_renting"]), t["is_returning"])
            empty = pc.and_(pc.and_(operational, pc.equal(t["num_bikes_available"], 0)), pc.invert(t["is_stale"]))
            ids = t["station_id"].to_pylist()
            obs_by_station.update(ids)
            e_ids = t.filter(empty)["station_id"].to_pylist()
            empty_by_station.update(e_ids)
            obs_by_hour[h] += t.num_rows
            empty_by_hour[h] += len(e_ids)

        top = [EmptyStation(station_id=sid, empty_share=round(n / float(obs_by_station[sid]), 4),
                            empty_observations=n)
               for sid, n in empty_by_station.most_common(10)]
        return SectionStatus(
            observations=rows,
            distinct_stations=len(stations),
            stale_share=round(stale / float(rows), 4) if rows else None,
            repeat_share=round(repeats / float(rows), 4) if rows else None,
            out_of_order=ooo,
            unknown_station_rows=unknown,
            age_cdf={str(e): round(age_le[e] / float(rows), 4) for e in AGE_EDGES_S} if rows else {},
            top_empty_stations=top,
            empty_share_by_hour_utc={str(h): round(empty_by_hour[h] / float(obs_by_hour[h]), 4)
                                     for h in sorted(obs_by_hour)},
        )
