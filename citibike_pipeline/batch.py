"""Micro-batch logic: plan -> process -> metrics -> finalize.

Idempotency: a plan is identified by where it starts (the committed offsets), not by the Airflow run.
Until finalize commits, every run gets the same plan back: a task retry, and also the next scheduled
run after a run failed for good. Every output key derives from batch_id (a hash of the planned offset
ranges) and offsets are committed last, so any rerun re-reads the same offsets and overwrites the
same keys instead of writing a second copy under a new batch_id.
"""
from __future__ import annotations

import bisect
import hashlib
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa

from citibike_contract import EVENT_TYPE_BY_TOPIC, Envelope, InvalidEnvelope, decode

from . import lake
from .config import DATA_TOPICS, PipelineConfig
from .kafka_io import (
    KafkaBatchReader,
    Record,
    cap_ranges,
    fits_within,
    ranges_from_json,
    ranges_to_json,
    total_messages,
)
from .metrics import Obs, pct, station_state, window_metrics
from .schemas import (
    BATCH_LOG,
    CURRENT_STATE,
    DLQ,
    SYSTEM_AVAILABILITY,
    WINDOW_15M,
    to_table,
)
from .storage import Storage, get_json, put_json, put_parquet, read_parquet
from .timeutil import epoch, floor_5min, from_epoch, iso, parse_iso, utcnow
from .validation import (
    validate_poll_log_payload,
    validate_status_payload,
    validate_weather_payload,
)

log = logging.getLogger(__name__)

STATUS_CONTEXT_COLS = ["station_id", "fetched_at", "last_reported"]
WEATHER_CONTEXT_COLS = ["obs_time", "fetched_at", "temperature_c", "precipitation_mm", "weather_code",
                        "wind_speed_kmh"]


def safe_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


# ---------------------------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------------------------

PENDING_PLAN = "ops/batch_plan/pending.json"


def plan_batch(run_id: str, reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
               now: Optional[datetime] = None) -> Dict[str, Any]:
    """The batch to process next. Reuses the pending (planned, not yet committed) plan when it still
    starts at the committed offsets and all its offsets still exist, so a rerun of a failed batch keeps
    its batch_id. Otherwise, e.g. after a broker reset recreated the topics, it plans afresh."""
    positions = reader.positions(DATA_TOPICS)
    pending = get_json(storage, PENDING_PLAN) if storage.exists(PENDING_PLAN) else None
    if pending is not None and fits_within(ranges_from_json(pending["ranges"]), positions):
        plan = dict(pending, run_id=run_id)
        if pending["first_run_id"] != run_id:
            log.warning("reusing uncommitted plan %s first made by run %s", plan["batch_id"], plan["first_run_id"])
    else:
        ranges = cap_ranges(positions, cfg.max_batch_per_partition)
        rj = ranges_to_json(ranges)
        batch_id = "b" + hashlib.sha1(json.dumps(rj, sort_keys=True).encode()).hexdigest()[:12]
        plan = {"run_id": run_id, "first_run_id": run_id, "batch_id": batch_id, "planned_at": iso(now or utcnow()),
                "ranges": rj, "total_messages": total_messages(ranges)}
        if plan["total_messages"]:
            put_json(storage, PENDING_PLAN, plan)
    put_json(storage, "ops/batch_plan/%s.json" % safe_id(run_id), plan)      # per-run audit record
    return plan


# ---------------------------------------------------------------------------------------------
# process
# ---------------------------------------------------------------------------------------------

@dataclass
class Parsed:
    raw_rows: List[Dict[str, Any]] = field(default_factory=list)
    dlq_rows: List[Dict[str, Any]] = field(default_factory=list)
    status: List[Tuple[Record, Envelope]] = field(default_factory=list)
    weather: List[Tuple[Record, Envelope]] = field(default_factory=list)
    records_read: int = 0


def _ts(ms: Optional[int]) -> Optional[datetime]:
    return from_epoch(ms / 1000.0) if ms is not None else None


def _dlq(rec: Record, reason: str, batch_id: str, processed_at: datetime) -> Dict[str, Any]:
    return {"reason": reason, "kafka_topic": rec.topic, "kafka_partition": rec.partition,
            "kafka_offset": rec.offset, "kafka_timestamp": _ts(rec.timestamp_ms),
            "raw_value": (rec.value or b"").decode("utf-8", "replace"),
            "batch_id": batch_id, "processed_at": processed_at}


def parse_records(records: List[Record], batch_id: str, processed_at: datetime,
                  cfg: PipelineConfig) -> Parsed:
    out = Parsed(records_read=len(records))
    for rec in records:
        try:
            env = decode(rec.value)
        except InvalidEnvelope as e:
            out.dlq_rows.append(_dlq(rec, ",".join(e.reasons), batch_id, processed_at))
            continue
        et, payload, fetched = env.event_type, env.payload, env.fetched_at
        if EVENT_TYPE_BY_TOPIC.get(rec.topic) != et:
            out.dlq_rows.append(_dlq(rec, "event_type_topic_mismatch", batch_id, processed_at))
            continue
        if et == "station_status":
            reasons = validate_status_payload(payload, epoch(fetched), cfg.future_skew_s)
        elif et == "weather_current":
            reasons = validate_weather_payload(payload)
        else:
            reasons = validate_poll_log_payload(payload)
        out.raw_rows.append({
            "kafka_topic": rec.topic, "kafka_partition": rec.partition, "kafka_offset": rec.offset,
            "kafka_timestamp": _ts(rec.timestamp_ms), "event_type": et, "poll_id": env.poll_id,
            "source": env.source, "source_url": env.source_url,
            "gbfs_version": env.gbfs_version, "feed_last_updated": env.feed_last_updated,
            "fetched_at": fetched, "producer_version": env.producer_version,
            "replay": env.replay, "replayed_at": env.replayed_at,
            "payload_json": json.dumps(payload, separators=(",", ":")),
            "batch_id": batch_id, "processed_at": processed_at,
        })
        if reasons:
            out.dlq_rows.append(_dlq(rec, ",".join(reasons), batch_id, processed_at))
            continue
        if et == "station_status":
            out.status.append((rec, env))
        elif et == "weather_current":
            out.weather.append((rec, env))
    return out


def _weather_rows(items, batch_id: str, processed_at: datetime) -> List[Dict[str, Any]]:
    rows = []
    for rec, env in items:
        p = env.payload
        cur = p["current"]
        rows.append({
            "location_id": p.get("location_id"), "obs_time": from_epoch(cur["time"]), "fetched_at": env.fetched_at,
            "temperature_c": cur.get("temperature_2m"), "relative_humidity": cur.get("relative_humidity_2m"),
            "precipitation_mm": cur.get("precipitation"), "rain_mm": cur.get("rain"),
            "snowfall_cm": cur.get("snowfall"), "weather_code": cur.get("weather_code"),
            "cloud_cover": cur.get("cloud_cover"), "wind_speed_kmh": cur.get("wind_speed_10m"),
            "poll_id": env.poll_id, "kafka_topic": rec.topic, "kafka_partition": rec.partition,
            "kafka_offset": rec.offset, "batch_id": batch_id, "processed_at": processed_at,
        })
    return rows


def build_status_rows(parsed: Parsed, prior: Optional[pa.Table], dim: Optional[Dict[str, Dict[str, Any]]],
                      weather_obs: List[Dict[str, Any]], batch_id: str, processed_at: datetime,
                      cfg: PipelineConfig) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Dedup, flag, and enrich station observations in deterministic (time, partition, offset) order."""
    max_lr: Dict[str, int] = {}
    seen: Set[Tuple[str, int]] = set()
    if prior is not None and prior.num_rows:
        for sid, lr in zip(prior["station_id"].to_pylist(), prior["last_reported"].to_pylist()):
            if lr is None:
                continue
            seen.add((sid, lr))
            if lr > max_lr.get(sid, -1):
                max_lr[sid] = lr

    wx = sorted(weather_obs, key=lambda w: w["obs_time"])
    wx_t = [epoch(w["obs_time"]) for w in wx]

    stats = {"duplicates_dropped": 0, "repeats": 0, "out_of_order": 0, "stale": 0,
             "unknown_station": 0, "replay_rows": 0}
    rows: List[Dict[str, Any]] = []
    delivered: Set[Tuple[str, str]] = set()
    stale_s = cfg.stale_minutes * 60
    for rec, env in sorted(parsed.status, key=lambda x: (x[1].fetched_at, x[0].partition, x[0].offset)):
        p, fetched = env.payload, env.fetched_at
        sid = p["station_id"]
        dup_key = (env.poll_id, sid)
        if dup_key in delivered:
            stats["duplicates_dropped"] += 1
            continue
        delivered.add(dup_key)

        lr = p["last_reported"]
        is_repeat = (sid, lr) in seen
        is_ooo = lr < max_lr.get(sid, -1)
        seen.add((sid, lr))
        max_lr[sid] = max(lr, max_lr.get(sid, -1))

        age_s = int(epoch(fetched)) - lr
        is_stale = age_s > stale_s
        d = dim.get(sid) if dim is not None else None
        unknown = dim is not None and d is None

        w = None
        idx = bisect.bisect_right(wx_t, epoch(fetched)) - 1
        if idx >= 0:
            w = wx[idx]

        stats["repeats"] += is_repeat
        stats["out_of_order"] += is_ooo
        stats["stale"] += is_stale
        stats["unknown_station"] += unknown
        stats["replay_rows"] += env.replay
        rows.append({
            "station_id": sid, "fetched_at": fetched, "last_reported": lr,
            "feed_last_updated": env.feed_last_updated,
            "num_bikes_available": p["num_bikes_available"], "num_ebikes_available": p.get("num_ebikes_available"),
            "num_docks_available": p["num_docks_available"], "num_bikes_disabled": p.get("num_bikes_disabled"),
            "num_docks_disabled": p.get("num_docks_disabled"),
            "is_installed": bool(p.get("is_installed", 1)), "is_renting": bool(p.get("is_renting", 1)),
            "is_returning": bool(p.get("is_returning", 1)),
            "age_s": age_s, "is_stale": is_stale, "is_repeat": is_repeat, "is_out_of_order": is_ooo,
            "is_unknown_station": unknown,
            "station_name": d.get("name") if d else None, "lat": d.get("lat") if d else None,
            "lon": d.get("lon") if d else None, "capacity": d.get("capacity") if d else None,
            "temperature_c": w["temperature_c"] if w else None,
            "precipitation_mm": w["precipitation_mm"] if w else None,
            "weather_code": w["weather_code"] if w else None,
            "wind_speed_kmh": w["wind_speed_kmh"] if w else None,
            "poll_id": env.poll_id, "replay": env.replay, "replayed_at": env.replayed_at,
            "kafka_topic": rec.topic, "kafka_partition": rec.partition, "kafka_offset": rec.offset,
            "kafka_timestamp": _ts(rec.timestamp_ms), "batch_id": batch_id, "processed_at": processed_at,
        })
    return rows, stats


def _load_dim(storage: Storage) -> Optional[Dict[str, Dict[str, Any]]]:
    if not storage.exists(lake.DIM_STATION_CURRENT):
        return None
    t = read_parquet(storage, lake.DIM_STATION_CURRENT)
    return {r["station_id"]: r for r in t.to_pylist()}


def _choose_as_of(parsed: Parsed, records: List[Record], processed_at: datetime) -> datetime:
    """Event-time horizon of the batch. Status observations define it: weather and poll_log sit on
    their own partitions and can be ahead of the status partitions during catch-up or replay."""
    if parsed.status:
        return max(e.fetched_at for _r, e in parsed.status)
    if parsed.weather:
        return max(e.fetched_at for _r, e in parsed.weather)
    if parsed.raw_rows:
        return max(r["fetched_at"] for r in parsed.raw_rows)
    kafka_ms = [r.timestamp_ms for r in records if r.timestamp_ms]
    return from_epoch(max(kafka_ms) / 1000.0) if kafka_ms else processed_at


def process_batch(plan: Dict[str, Any], reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
                  now: Optional[datetime] = None) -> Dict[str, Any]:
    batch_id = plan["batch_id"]
    processed_at = now or utcnow()
    records = reader.read(ranges_from_json(plan["ranges"]), cfg.read_timeout_s)
    parsed = parse_records(records, batch_id, processed_at, cfg)

    as_of = _choose_as_of(parsed, records, processed_at)
    status_times = [e.fetched_at for _r, e in parsed.status]
    all_times = status_times + [e.fetched_at for _r, e in parsed.weather] + [as_of]
    ctx_end = min(status_times) if status_times else as_of
    prior = lake.CURATED_STATUS.read_window(storage, ctx_end - timedelta(minutes=cfg.context_minutes), ctx_end,
                                            STATUS_CONTEXT_COLS, exclude_batch=batch_id) if parsed.status else None
    # Weather from earlier batches can be newer than this batch's first status row (partitions drain at
    # different speeds), so read it up to the newest time seen here.
    prior_weather_t = lake.CURATED_WEATHER.read_window(storage, ctx_end - timedelta(hours=3),
                                                       max(all_times) + timedelta(milliseconds=1),
                                                       WEATHER_CONTEXT_COLS, exclude_batch=batch_id)
    weather_rows = _weather_rows(parsed.weather, batch_id, processed_at)
    weather_obs = prior_weather_t.to_pylist() + weather_rows

    status_rows, stats = build_status_rows(parsed, prior, _load_dim(storage), weather_obs, batch_id,
                                           processed_at, cfg)

    # ---- writes: deterministic keys, overwrite-safe ------------------------------------------
    by_event: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in parsed.raw_rows:
        by_event[r["event_type"]].append(r)
    for et, rows in by_event.items():
        lake.RAW_BY_EVENT_TYPE[et].write(storage, batch_id, rows)
    status_hours = lake.CURATED_STATUS.write(storage, batch_id, status_rows)
    lake.CURATED_WEATHER.write(storage, batch_id, weather_rows)
    if parsed.dlq_rows:
        put_parquet(storage, lake.part_key(lake.DLQ, as_of, "%s.parquet" % batch_id), to_table(parsed.dlq_rows, DLQ))

    stats.update({
        "batch_id": batch_id, "as_of": iso(as_of), "records_read": len(records),
        "raw_rows": len(parsed.raw_rows), "status_rows": len(status_rows),
        "weather_rows": len(weather_rows), "dlq_rows": len(parsed.dlq_rows),
        "status_hours": [iso(h) for h in status_hours],
    })
    log.info("batch %s: read=%d raw=%d status=%d weather=%d dlq=%d as_of=%s", batch_id, len(records),
             len(parsed.raw_rows), len(status_rows), len(weather_rows), len(parsed.dlq_rows), stats["as_of"])
    return stats


# ---------------------------------------------------------------------------------------------
# metrics + serving files
# ---------------------------------------------------------------------------------------------

METRIC_COLS = [
    "kafka_partition", "kafka_offset",                 # tie-breakers for the observation order
    "station_id", "fetched_at", "last_reported", "num_bikes_available", "num_ebikes_available",
    "num_docks_available", "is_installed", "is_renting", "is_returning", "station_name", "lat", "lon",
    "capacity", "temperature_c", "precipitation_mm",
]


def compute_metrics(plan: Dict[str, Any], stats: Dict[str, Any], storage: Storage, cfg: PipelineConfig,
                    now: Optional[datetime] = None) -> Dict[str, Any]:
    batch_id = plan["batch_id"]
    if not stats.get("status_rows"):
        return {"published_at": None, "stations": 0}
    as_of = parse_iso(stats["as_of"])
    if storage.exists(lake.SERVING_SUMMARY):
        # A catch-up batch can end earlier than the previous one (partitions drain at different speeds).
        # Serving time never moves backwards; a retry sees its own earlier value and stays stable.
        prev = read_parquet(storage, lake.SERVING_SUMMARY, columns=["as_of"])["as_of"].to_pylist()
        if prev and prev[0] > as_of:
            as_of = prev[0]
    start = as_of - timedelta(minutes=cfg.context_minutes)
    table = lake.CURATED_STATUS.read_window(storage, start, as_of + timedelta(milliseconds=1), METRIC_COLS)
    cols = {c: table[c].to_pylist() for c in table.column_names}
    n = table.num_rows

    obs: Dict[str, List[Obs]] = defaultdict(list)
    latest: Dict[str, int] = {}
    order = list(range(n))
    order.sort(key=lambda i: (cols["fetched_at"][i], cols["kafka_partition"][i], cols["kafka_offset"][i]))
    for i in order:
        sid = cols["station_id"][i]
        obs[sid].append(Obs(epoch(cols["fetched_at"][i]), cols["num_bikes_available"][i],
                            cols["num_docks_available"][i]))
        latest[sid] = i

    as_of_s = epoch(as_of)
    stale_s = cfg.stale_minutes * 60
    window_rows: List[Dict[str, Any]] = []
    state_rows: List[Dict[str, Any]] = []
    for sid, i in sorted(latest.items()):
        wm = window_metrics(obs[sid], as_of_s, cfg)
        age = int(as_of_s) - cols["last_reported"][i]
        state = station_state(cols["is_installed"][i], cols["is_renting"][i], cols["is_returning"][i],
                              age > stale_s, cols["num_bikes_available"][i], cols["num_docks_available"][i])
        at_risk = None
        if wm:
            window_rows.append({"station_id": sid, "as_of": as_of, "window_start": from_epoch(wm.pop("window_start")),
                                "batch_id": batch_id, **wm})
            if state == "OK":
                at_risk = "EMPTY" if wm["at_risk_empty"] else ("FULL" if wm["at_risk_full"] else None)
        state_rows.append({
            "station_id": sid, "station_name": cols["station_name"][i], "lat": cols["lat"][i],
            "lon": cols["lon"][i], "capacity": cols["capacity"][i], "state": state,
            "num_bikes_available": cols["num_bikes_available"][i],
            "num_ebikes_available": cols["num_ebikes_available"][i],
            "num_docks_available": cols["num_docks_available"][i], "last_reported": cols["last_reported"][i],
            "age_s": age, "observed_at": cols["fetched_at"][i],
            "net_change_15m": wm["net_change"] if wm else None,
            "rate_per_min": wm["rate_per_min"] if wm else None,
            "minutes_to_empty": wm["minutes_to_empty"] if wm else None,
            "minutes_to_full": wm["minutes_to_full"] if wm else None,
            "at_risk": at_risk, "temperature_c": cols["temperature_c"][i],
            "precipitation_mm": cols["precipitation_mm"][i], "as_of": as_of, "batch_id": batch_id,
        })

    total = len(state_rows)
    operational = [r for r in state_rows if r["state"] != "OFFLINE"]
    n_op = len(operational) or 1
    share = lambda pred, pop, denom: (sum(1 for r in pop if pred(r)) / float(denom)) if denom else None  # noqa: E731
    latest_i = max(latest.values(), key=lambda i: (cols["fetched_at"][i], cols["kafka_offset"][i]))
    bucket = floor_5min(as_of)
    availability = {
        "bucket_start": bucket, "as_of": as_of, "n_stations": total, "n_operational": len(operational),
        "share_with_bike": share(lambda r: r["num_bikes_available"] > 0, operational, n_op),
        "share_with_dock": share(lambda r: r["num_docks_available"] > 0, operational, n_op),
        "share_empty": share(lambda r: r["state"] == "EMPTY", state_rows, total),
        "share_full": share(lambda r: r["state"] == "FULL", state_rows, total),
        "share_stale": share(lambda r: r["state"] == "STALE", state_rows, total),
        "share_offline": share(lambda r: r["state"] == "OFFLINE", state_rows, total),
        "n_at_risk_empty": sum(1 for r in state_rows if r["at_risk"] == "EMPTY"),
        "n_at_risk_full": sum(1 for r in state_rows if r["at_risk"] == "FULL"),
        "temperature_c": cols["temperature_c"][latest_i], "precipitation_mm": cols["precipitation_mm"][latest_i],
        "batch_id": batch_id,
    }
    at_risk_rows = sorted((r for r in state_rows if r["at_risk"]),
                          key=lambda r: min(x for x in (r["minutes_to_empty"], r["minutes_to_full"]) if x is not None))

    # history first, serving files last (they are what the dashboard reads)
    put_parquet(storage, lake.part_key(lake.GOLD_WINDOW, as_of, "%s.parquet" % batch_id),
                to_table(window_rows, WINDOW_15M))
    put_parquet(storage, lake.part_key(lake.GOLD_AVAILABILITY, as_of, "%s.parquet" % bucket.strftime("%H%M")),
                to_table([availability], SYSTEM_AVAILABILITY))
    put_parquet(storage, lake.SERVING_AT_RISK, to_table(at_risk_rows, CURRENT_STATE))
    put_parquet(storage, lake.SERVING_SUMMARY, to_table([availability], SYSTEM_AVAILABILITY))
    put_parquet(storage, lake.SERVING_CURRENT, to_table(state_rows, CURRENT_STATE))
    log.info("batch %s: published %d stations, %d at risk", plan["batch_id"], total, len(at_risk_rows))
    return {"published_at": iso(now or utcnow()), "stations": total, "at_risk": len(at_risk_rows)}


# ---------------------------------------------------------------------------------------------
# finalize
# ---------------------------------------------------------------------------------------------

def batch_latencies(storage: Storage, plan: Dict[str, Any], stats: Dict[str, Any],
                    published_at: Optional[datetime]) -> List[float]:
    """Seconds from observation (or replay time) to the moment the serving files were written."""
    if published_at is None or not stats.get("status_rows"):
        return []
    if "status_hours" in stats:
        hours = [parse_iso(h) for h in stats["status_hours"]]
    elif "status_keys" in stats:            # stats from a run started before the deploy that added hours
        hours = lake.hours_from_legacy_keys(stats["status_keys"])
    else:
        raise KeyError("stats of batch %s have status rows but neither status_hours nor status_keys"
                       % plan["batch_id"])
    t = lake.CURATED_STATUS.read_batch(storage, plan["batch_id"], hours, ["fetched_at", "replayed_at"])
    return [max((published_at - (r or f)).total_seconds(), 0.0)
            for f, r in zip(t["fetched_at"].to_pylist(), t["replayed_at"].to_pylist())]


def finalize_batch(plan: Dict[str, Any], stats: Dict[str, Any], publish: Dict[str, Any],
                   reader: KafkaBatchReader, storage: Storage, cfg: PipelineConfig,
                   now: Optional[datetime] = None) -> Dict[str, Any]:
    published_at = parse_iso(publish["published_at"]) if publish.get("published_at") else None
    lat = sorted(batch_latencies(storage, plan, stats, published_at))
    finished = now or utcnow()
    as_of = parse_iso(stats["as_of"])
    entry = {
        "batch_id": plan["batch_id"], "run_id": plan["run_id"], "as_of": as_of,
        "planned_at": parse_iso(plan["planned_at"]), "published_at": published_at, "finished_at": finished,
        "records_read": stats["records_read"], "raw_rows": stats["raw_rows"], "status_rows": stats["status_rows"],
        "weather_rows": stats["weather_rows"], "dlq_rows": stats["dlq_rows"],
        "duplicates_dropped": stats["duplicates_dropped"], "repeats": stats["repeats"],
        "out_of_order": stats["out_of_order"], "stale": stats["stale"],
        "unknown_station": stats["unknown_station"], "replay_rows": stats["replay_rows"],
        "latency_p50_s": pct(lat, 0.50), "latency_p95_s": pct(lat, 0.95),
        "latency_max_s": lat[-1] if lat else None, "status": "success",
    }
    # per-batch record (authoritative history) keyed by batch_id: retries overwrite
    put_parquet(storage, lake.part_key(lake.BATCH_LOG, as_of, "%s.parquet" % plan["batch_id"]),
                to_table([entry], BATCH_LOG))
    # rolling table for the health panel: dedupe by batch_id, keep the last 24 h
    recent: List[Dict[str, Any]] = []
    if storage.exists(lake.SERVING_BATCH_LOG):
        recent = [r for r in read_parquet(storage, lake.SERVING_BATCH_LOG).to_pylist()
                  if r["batch_id"] != entry["batch_id"]]
    recent.append(entry)
    cutoff = as_of - timedelta(hours=24)
    recent = sorted((r for r in recent if r["as_of"] >= cutoff), key=lambda r: r["as_of"])
    put_parquet(storage, lake.SERVING_BATCH_LOG, to_table(recent, BATCH_LOG))

    reader.commit(ranges_from_json(plan["ranges"]))          # last: only after every output exists
    if storage.exists(PENDING_PLAN) and get_json(storage, PENDING_PLAN)["batch_id"] == plan["batch_id"]:
        storage.delete(PENDING_PLAN)      # tidy only: after the commit its starts no longer match anyway
    log.info("batch %s: committed offsets, latency p50=%s p95=%s", plan["batch_id"], entry["latency_p50_s"],
             entry["latency_p95_s"])
    return {k: (iso(v) if isinstance(v, datetime) else v) for k, v in entry.items()}
