"""The Snapshot: station states, windows and system availability for one batch at one as-of time.

Pure: no storage, no Kafka. `compute_snapshot` takes the curated status rows of the context window and returns
a value; `serving.publish` is what writes it.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

import pyarrow as pa

from .config import PipelineConfig
from .obs import Obs
from .station_state import StationState, station_state
from .station_window import StationWindow
from .timeutil import epoch, floor_5min
from .window_metrics import WindowMetrics

# The curated status columns a snapshot needs; `StatusRow.from_table` reads exactly these.
STATUS_ROW_COLS = [
    "kafka_partition", "kafka_offset",                 # tie-breakers for the observation order
    "station_id", "fetched_at", "last_reported", "num_bikes_available", "num_ebikes_available",
    "num_docks_available", "is_installed", "is_renting", "is_returning", "station_name", "lat", "lon",
    "capacity", "temperature_c", "precipitation_mm",
]


@dataclass(frozen=True)
class StatusRow:
    """One curated station-status observation, as far as the snapshot reads it."""
    kafka_partition: int
    kafka_offset: int
    station_id: str
    fetched_at: datetime
    last_reported: int
    num_bikes_available: int
    num_ebikes_available: int
    num_docks_available: int
    is_installed: bool
    is_renting: bool
    is_returning: bool
    station_name: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    capacity: Optional[int]
    temperature_c: Optional[float]
    precipitation_mm: Optional[float]

    @staticmethod
    def from_table(table: pa.Table) -> List[StatusRow]:
        cols = {c: table[c].to_pylist() for c in STATUS_ROW_COLS}
        return [StatusRow(**{c: cols[c][i] for c in STATUS_ROW_COLS}) for i in range(table.num_rows)]


@dataclass(frozen=True)
class SystemAvailability:
    bucket_start: datetime
    as_of: datetime
    n_stations: int
    n_operational: int
    share_with_bike: Optional[float]
    share_with_dock: Optional[float]
    share_empty: Optional[float]
    share_full: Optional[float]
    share_stale: Optional[float]
    share_offline: Optional[float]
    n_at_risk_empty: int
    n_at_risk_full: int
    temperature_c: Optional[float]
    precipitation_mm: Optional[float]
    batch_id: str


@dataclass(frozen=True)
class Snapshot:
    as_of: datetime
    batch_id: str
    states: List[StationState]
    windows: List[StationWindow]
    availability: SystemAvailability
    at_risk: List[StationState]     # most urgent first


def _urgency(s: StationState) -> float:
    return min(x for x in (s.minutes_to_empty, s.minutes_to_full) if x is not None)


def _share(pred: Callable[[StationState], bool], pop: List[StationState], denom: int) -> Optional[float]:
    return sum(1 for s in pop if pred(s)) / float(denom) if denom else None


def compute_snapshot(rows: List[StatusRow], as_of: datetime, batch_id: str,
                     cfg: PipelineConfig) -> Optional[Snapshot]:
    """None when there are no rows (nothing to publish)."""
    if not rows:
        return None
    ordered = sorted(rows, key=lambda r: (r.fetched_at, r.kafka_partition, r.kafka_offset))
    obs = defaultdict(list)  # station_id -> List[Obs]
    latest = {}  # station_id -> its newest StatusRow
    for r in ordered:
        obs[r.station_id].append(Obs(epoch(r.fetched_at), r.num_bikes_available, r.num_docks_available))
        latest[r.station_id] = r

    as_of_s = epoch(as_of)
    stale_s = cfg.stale_minutes * 60
    windows: List[StationWindow] = []
    states: List[StationState] = []
    for sid, r in sorted(latest.items()):
        m = WindowMetrics.over(obs[sid], as_of_s, cfg)
        age = int(as_of_s) - r.last_reported
        state = station_state(r.is_installed, r.is_renting, r.is_returning, age > stale_s,
                              r.num_bikes_available, r.num_docks_available)
        if m:
            windows.append(StationWindow.from_metrics(sid, as_of, batch_id, m))
        states.append(StationState(
            station_id=sid, station_name=r.station_name, lat=r.lat, lon=r.lon, capacity=r.capacity,
            state=state, num_bikes_available=r.num_bikes_available,
            num_ebikes_available=r.num_ebikes_available, num_docks_available=r.num_docks_available,
            last_reported=r.last_reported, age_s=age, observed_at=r.fetched_at,
            net_change_15m=m.net_change if m else None,
            rate_per_min=m.rate_per_min if m else None,
            minutes_to_empty=m.minutes_to_empty if m else None,
            minutes_to_full=m.minutes_to_full if m else None,
            at_risk=m.at_risk if m and state == "OK" else None,   # only an OK station can be at risk
            temperature_c=r.temperature_c, precipitation_mm=r.precipitation_mm,
            as_of=as_of, batch_id=batch_id))

    total = len(states)
    operational = [s for s in states if s.state != "OFFLINE"]
    n_op = len(operational) or 1
    newest = max(latest.values(), key=lambda r: (r.fetched_at, r.kafka_offset))
    availability = SystemAvailability(
        bucket_start=floor_5min(as_of), as_of=as_of, n_stations=total, n_operational=len(operational),
        share_with_bike=_share(lambda s: s.num_bikes_available > 0, operational, n_op),
        share_with_dock=_share(lambda s: s.num_docks_available > 0, operational, n_op),
        share_empty=_share(lambda s: s.state == "EMPTY", states, total),
        share_full=_share(lambda s: s.state == "FULL", states, total),
        share_stale=_share(lambda s: s.state == "STALE", states, total),
        share_offline=_share(lambda s: s.state == "OFFLINE", states, total),
        n_at_risk_empty=sum(1 for s in states if s.at_risk == "EMPTY"),
        n_at_risk_full=sum(1 for s in states if s.at_risk == "FULL"),
        temperature_c=newest.temperature_c, precipitation_mm=newest.precipitation_mm, batch_id=batch_id)
    at_risk = sorted((s for s in states if s.at_risk), key=_urgency)
    return Snapshot(as_of=as_of, batch_id=batch_id, states=states, windows=windows,
                    availability=availability, at_risk=at_risk)
