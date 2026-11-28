"""Micro-batch logic: plan -> process -> metrics -> finalize.

Idempotency: a plan is identified by where it starts (the committed offsets), not by the Airflow run.
Until finalize commits, every run gets the same plan back: a task retry, and also the next scheduled
run after a run failed for good. Every output key derives from batch_id (a hash of the planned offset
ranges) and offsets are committed last, so any rerun re-reads the same offsets and overwrites the
same keys instead of writing a second copy under a new batch_id.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from citibike_contract import JsonObject

from . import lake, serving
from .batch_log import BatchLog, BatchLogEntry, Latencies
from .config import DATA_TOPICS, PipelineConfig
from .decoded_batch import DecodedBatch
from .enrichment import enrich_status
from .enrichment_context import EnrichmentContext
from .hour_partitions import part_key
from .jsonfields import as_int, as_object
from .kafka_batch_reader import KafkaBatchReader
from .plan import Plan
from .ranges import cap_ranges, fits_within, ranges_to_json, total_messages
from .rows import CuratedWeatherRow
from .schemas import (
    DLQ,
    to_table,
)
from .snapshot import STATUS_ROW_COLS, StatusRow, compute_snapshot
from .station_information_table import StationInformationTable
from .stats import Stats
from .storage import Storage
from .storage_ops import get_json, put_json, put_parquet
from .timeutil import iso, parse_iso, utcnow

log = logging.getLogger(__name__)


def safe_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


# ---------------------------------------------------------------------------------------------
# records passed between the steps
# ---------------------------------------------------------------------------------------------
# Each record has to_json/from_json so it can travel through Airflow XCom. The JSON keys are the ones the
# steps used to pass as dicts, so values already in flight and the files under ops/batch_plan/ stay readable.
# from_json indexes every field (a missing one raises KeyError), except Published.at_risk: old in-flight XCom lacks it.


@dataclass(frozen=True)
class Published:
    """What compute_metrics wrote to the serving files; published_at is None when the batch had no status rows."""
    published_at: Optional[datetime]
    stations: int
    at_risk: int = 0

    def to_json(self) -> JsonObject:
        return {"published_at": iso(self.published_at) if self.published_at else None,
                "stations": self.stations, "at_risk": self.at_risk}

    @classmethod
    def from_json(cls, obj: JsonObject) -> Published:
        published_at = obj["published_at"]
        return cls(published_at=parse_iso(str(published_at)) if published_at else None,
                   stations=as_int(obj["stations"]), at_risk=as_int(obj.get("at_risk", 0)))


# ---------------------------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------------------------

PENDING_PLAN = "ops/batch_plan/pending.json"


def _pending_plan(storage: Storage) -> Optional[Plan]:
    """The plan stored as pending (planned, offsets not yet committed), or None."""
    return Plan.from_json(as_object(get_json(storage, PENDING_PLAN))) if storage.exists(PENDING_PLAN) else None


def plan_batch(run_id: str, reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
               now: Optional[datetime] = None) -> Plan:
    """The batch to process next. Reuses the pending (planned, not yet committed) plan when it still
    starts at the committed offsets and all its offsets still exist, so a rerun of a failed batch keeps
    its batch_id. Otherwise, e.g. after a broker reset recreated the topics, it plans afresh."""
    positions = reader.positions(DATA_TOPICS)
    pending = _pending_plan(storage)
    if pending is not None and fits_within(pending.ranges, positions):
        plan = replace(pending, run_id=run_id)
        if pending.first_run_id != run_id:
            log.warning("reusing uncommitted plan %s first made by run %s", plan.batch_id, plan.first_run_id)
    else:
        ranges = cap_ranges(positions, cfg.max_batch_per_partition)
        batch_id = "b" + hashlib.sha1(json.dumps(ranges_to_json(ranges), sort_keys=True).encode()).hexdigest()[:12]
        planned_at = parse_iso((now or utcnow()).isoformat(timespec="milliseconds"))   # to the millisecond, as stored
        plan = Plan(run_id=run_id, first_run_id=run_id, batch_id=batch_id, planned_at=planned_at,
                    ranges=ranges, total_messages=total_messages(ranges))
        if plan.has_data:
            put_json(storage, PENDING_PLAN, plan.to_json())
    put_json(storage, "ops/batch_plan/%s.json" % safe_id(run_id), plan.to_json())      # per-run audit record
    return plan


# ---------------------------------------------------------------------------------------------
# process
# ---------------------------------------------------------------------------------------------

def process_batch(plan: Plan, reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
                  now: Optional[datetime] = None) -> Stats:
    batch_id = plan.batch_id
    processed_at = now or utcnow()
    records = reader.read(plan.ranges, cfg.read_timeout_s)
    decoded = DecodedBatch.decode(records, batch_id, processed_at, cfg)

    as_of = decoded.as_of
    context = EnrichmentContext.read(storage, decoded, batch_id, cfg)
    weather_rows = [CuratedWeatherRow.from_delivered(d, batch_id, processed_at) for d in decoded.weather]

    status_rows, counts = enrich_status(decoded.status, context.prior, StationInformationTable().current(storage),
                                        context.weather, batch_id, processed_at, cfg)

    # ---- writes: deterministic keys, overwrite-safe ------------------------------------------
    for et, rows in decoded.raw_rows_by_event_type().items():
        lake.RAW_BY_EVENT_TYPE[et].write(storage, batch_id, rows)
    status_hours = lake.CURATED_STATUS.write(storage, batch_id, status_rows)
    lake.CURATED_WEATHER.write(storage, batch_id, weather_rows)
    if decoded.dead_letters:
        put_parquet(storage, part_key(lake.DLQ, as_of, "%s.parquet" % batch_id),
                    to_table(decoded.dead_letters, DLQ))

    stats = Stats(batch_id=batch_id, as_of=as_of, status_hours=tuple(status_hours),
                  counts=replace(counts, records_read=len(records), raw_rows=len(decoded.raw_rows),
                                 weather_rows=len(weather_rows), dlq_rows=len(decoded.dead_letters)))
    log.info("batch %s: read=%d raw=%d status=%d weather=%d dlq=%d as_of=%s", batch_id, len(records),
             len(decoded.raw_rows), len(status_rows), len(weather_rows), len(decoded.dead_letters), iso(as_of))
    return stats


# ---------------------------------------------------------------------------------------------
# metrics + serving files
# ---------------------------------------------------------------------------------------------

def compute_metrics(plan: Plan, stats: Stats, storage: Storage, cfg: PipelineConfig,
                    now: Optional[datetime] = None) -> Published:
    if not stats.counts.status_rows:
        return Published(published_at=None, stations=0)
    as_of = serving.serving_as_of(storage, stats.as_of)
    start = as_of - timedelta(minutes=cfg.context_minutes)
    table = lake.CURATED_STATUS.read_window(storage, start, as_of + timedelta(milliseconds=1), STATUS_ROW_COLS)
    snap = compute_snapshot(StatusRow.from_table(table), as_of, plan.batch_id, cfg)
    if snap is None:
        return Published(published_at=None, stations=0)
    serving.publish(storage, snap)
    log.info("batch %s: published %d stations, %d at risk", plan.batch_id, len(snap.states), len(snap.at_risk))
    return Published(published_at=now or utcnow(), stations=len(snap.states), at_risk=len(snap.at_risk))


# ---------------------------------------------------------------------------------------------
# finalize
# ---------------------------------------------------------------------------------------------

def batch_latencies(storage: Storage, plan: Plan, stats: Stats,
                    published_at: Optional[datetime]) -> List[float]:
    """Seconds from observation (or replay time) to the moment the serving files were written."""
    if published_at is None or not stats.counts.status_rows:
        return []
    t = lake.CURATED_STATUS.read_batch(storage, plan.batch_id, list(stats.status_hours),
                                       ["fetched_at", "replayed_at"])
    return [max((published_at - (r or f)).total_seconds(), 0.0)
            for f, r in zip(t["fetched_at"].to_pylist(), t["replayed_at"].to_pylist())]


def finalize_batch(plan: Plan, stats: Stats, published: Published,  # noqa: PLR0913
                   reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
                   now: Optional[datetime] = None) -> None:
    """Write the Batch log entry, then commit the plan's offsets, last of all."""
    latencies = Latencies.from_seconds(batch_latencies(storage, plan, stats, published.published_at))
    entry = BatchLogEntry.from_batch(plan, stats, published, latencies, now or utcnow())
    BatchLog(storage).record(entry)

    reader.commit(plan.ranges)          # last: only after every output exists
    pending = _pending_plan(storage)
    if pending is not None and pending.batch_id == plan.batch_id:
        storage.delete(PENDING_PLAN)      # tidy only: after the commit its starts no longer match anyway
    log.info("batch %s: committed offsets, latency p50=%s p95=%s", plan.batch_id, latencies.p50_s,
             latencies.p95_s)


# ---------------------------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------------------------

# The steps of a batch, in the order run_batch takes them. The Airflow DAG has one task per step, in this order.
STEPS = ("plan", "process", "metrics", "finalize")


def run_batch(run_id: str, reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
              now: Optional[datetime] = None) -> Tuple[Plan, Optional[Stats]]:
    """Run one Airflow run's batch through every step (STEPS): plan, stop if the plan has no data, process,
    publish metrics, finalize with the offset commit last. Returns the plan and its stats, or None for the
    stats when there was nothing to read."""
    plan = plan_batch(run_id, reader, storage, cfg, now=now)
    if not plan.has_data:
        return plan, None
    stats = process_batch(plan, reader, storage, cfg, now=now)
    published = compute_metrics(plan, stats, storage, cfg, now=now)
    finalize_batch(plan, stats, published, reader, storage, cfg, now=now)
    return plan, stats
