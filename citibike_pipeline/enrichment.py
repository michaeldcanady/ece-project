"""Enrichment: the flags, dimension join and weather as-of join that turn a Batch's station statuses into curated
status rows. Pure: values in, values out. `enrichment_context.EnrichmentContext` does the lake reads it needs, and
`batch.process_batch` the writes.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from datetime import datetime
from typing import FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple

import pyarrow as pa

from .batch_log import Counts
from .config import PipelineConfig
from .observations import Delivered, StationStatus, WeatherObservation
from .rows import CuratedStatusRow
from .timeutil import epoch, from_epoch_ms

# The curated weather columns the as-of join reads; `WeatherReading.from_table` reads exactly these.
WEATHER_READING_COLS = ["obs_time", "temperature_c", "precipitation_mm", "weather_code", "wind_speed_kmh"]

# station id -> the station dimension's fields (candidate: a typed Station dimension module)
Dimension = Mapping[str, Mapping[str, object]]


@dataclass(frozen=True)
class PriorContext:
    """What earlier batches already recorded about each station, within the context window: the
    (station, last_reported) pairs seen, and the newest last_reported per station."""
    seen: FrozenSet[Tuple[str, int]] = frozenset()
    max_last_reported: Mapping[str, int] = field(default_factory=dict)

    @classmethod
    def from_table(cls, table: Optional[pa.Table]) -> PriorContext:
        """From curated status rows with `station_id` and `last_reported`; None means no prior rows."""
        if table is None or not table.num_rows:
            return cls()
        seen: Set[Tuple[str, int]] = set()
        newest = {}
        for station_id, last_reported in zip(table["station_id"].to_pylist(), table["last_reported"].to_pylist()):
            if last_reported is None:
                continue
            seen.add((station_id, last_reported))
            if last_reported > newest.get(station_id, -1):
                newest[station_id] = last_reported
        return cls(seen=frozenset(seen), max_last_reported=newest)


@dataclass(frozen=True)
class WeatherReading:
    """The weather at one observation time, as far as a station row reads it."""
    obs_time: datetime
    temperature_c: Optional[float]
    precipitation_mm: Optional[float]
    weather_code: Optional[int]
    wind_speed_kmh: Optional[float]

    @classmethod
    def from_observation(cls, obs: WeatherObservation) -> WeatherReading:
        return cls(obs.obs_time, obs.temperature_c, obs.precipitation_mm, obs.weather_code, obs.wind_speed_kmh)

    @staticmethod
    def from_table(table: pa.Table) -> List[WeatherReading]:
        cols = {c: table[c].to_pylist() for c in WEATHER_READING_COLS}
        return [WeatherReading(**{c: cols[c][i] for c in WEATHER_READING_COLS}) for i in range(table.num_rows)]


def enrich_status(statuses: Sequence[Delivered[StationStatus]], prior: PriorContext, dimension: Optional[Dimension],
                  weather: Sequence[WeatherReading], batch_id: str, processed_at: datetime,
                  cfg: PipelineConfig) -> Tuple[List[CuratedStatusRow], Counts]:
    """Dedup, flag, and enrich station observations in deterministic (time, partition, offset) order.

    `dimension` is None when no station dimension has been built yet (no station is then unknown). The Counts
    hold what this step sees (rows kept, dropped duplicates, flags); the caller fills in the rest."""
    seen = set(prior.seen)
    max_lr = dict(prior.max_last_reported)

    wx = sorted(weather, key=lambda w: w.obs_time)
    wx_t = [epoch(w.obs_time) for w in wx]

    duplicates_dropped = repeats = out_of_order = stale = unknown_station = replay_rows = 0
    rows: List[CuratedStatusRow] = []
    delivered: Set[Tuple[str, str]] = set()
    stale_s = cfg.stale_minutes * 60
    for d in sorted(statuses, key=lambda x: (x.envelope.fetched_at, x.record.partition, x.record.offset)):
        rec, env, st = d.record, d.envelope, d.value
        fetched = env.fetched_at
        sid = st.station_id
        dup_key = (env.poll_id, sid)
        if dup_key in delivered:
            duplicates_dropped += 1
            continue
        delivered.add(dup_key)

        lr = st.last_reported
        is_repeat = (sid, lr) in seen
        is_ooo = lr < max_lr.get(sid, -1)
        seen.add((sid, lr))
        max_lr[sid] = max(lr, max_lr.get(sid, -1))

        age_s = int(epoch(fetched)) - lr
        is_stale = age_s > stale_s
        info = dimension.get(sid) if dimension is not None else None
        unknown = dimension is not None and info is None

        w = None
        idx = bisect.bisect_right(wx_t, epoch(fetched)) - 1
        if idx >= 0:
            w = wx[idx]

        repeats += is_repeat
        out_of_order += is_ooo
        stale += is_stale
        unknown_station += unknown
        replay_rows += env.replay
        rows.append(CuratedStatusRow(
            station_id=sid, fetched_at=fetched, last_reported=lr, feed_last_updated=env.feed_last_updated,
            num_bikes_available=st.num_bikes_available, num_ebikes_available=st.num_ebikes_available,
            num_docks_available=st.num_docks_available, num_bikes_disabled=st.num_bikes_disabled,
            num_docks_disabled=st.num_docks_disabled, is_installed=st.is_installed, is_renting=st.is_renting,
            is_returning=st.is_returning, age_s=age_s, is_stale=is_stale, is_repeat=is_repeat,
            is_out_of_order=is_ooo, is_unknown_station=unknown,
            station_name=info.get("name") if info else None, lat=info.get("lat") if info else None,
            lon=info.get("lon") if info else None, capacity=info.get("capacity") if info else None,
            temperature_c=w.temperature_c if w else None, precipitation_mm=w.precipitation_mm if w else None,
            weather_code=w.weather_code if w else None, wind_speed_kmh=w.wind_speed_kmh if w else None,
            poll_id=env.poll_id, replay=env.replay, replayed_at=env.replayed_at, kafka_topic=rec.topic,
            kafka_partition=rec.partition, kafka_offset=rec.offset,
            kafka_timestamp=from_epoch_ms(rec.timestamp_ms), batch_id=batch_id, processed_at=processed_at))
    counts = Counts(records_read=0, raw_rows=0, status_rows=len(rows), weather_rows=0, dlq_rows=0,
                    duplicates_dropped=duplicates_dropped, repeats=repeats, out_of_order=out_of_order,
                    stale=stale, unknown_station=unknown_station, replay_rows=replay_rows)
    return rows, counts
