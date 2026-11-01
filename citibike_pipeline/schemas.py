"""Explicit Parquet schemas. Kafka coordinates (topic, partition, offset) are the natural row identity."""
from __future__ import annotations

from typing import Any, Dict, List

import pyarrow as pa

TS = pa.timestamp("ms", tz="UTC")

RAW = pa.schema([
    ("kafka_topic", pa.string()), ("kafka_partition", pa.int32()), ("kafka_offset", pa.int64()),
    ("kafka_timestamp", TS),
    ("event_type", pa.string()), ("poll_id", pa.string()), ("source", pa.string()),
    ("source_url", pa.string()), ("gbfs_version", pa.string()), ("feed_last_updated", pa.int64()),
    ("fetched_at", TS), ("producer_version", pa.string()), ("replay", pa.bool_()), ("replayed_at", TS),
    ("payload_json", pa.string()),            # lossless payload as received
    ("batch_id", pa.string()), ("processed_at", TS),
])

STATUS = pa.schema([
    ("station_id", pa.string()), ("fetched_at", TS), ("last_reported", pa.int64()),
    ("feed_last_updated", pa.int64()),
    ("num_bikes_available", pa.int32()), ("num_ebikes_available", pa.int32()),
    ("num_docks_available", pa.int32()), ("num_bikes_disabled", pa.int32()),
    ("num_docks_disabled", pa.int32()),
    ("is_installed", pa.bool_()), ("is_renting", pa.bool_()), ("is_returning", pa.bool_()),
    ("age_s", pa.int64()), ("is_stale", pa.bool_()), ("is_repeat", pa.bool_()),
    ("is_out_of_order", pa.bool_()), ("is_unknown_station", pa.bool_()),
    ("station_name", pa.string()), ("lat", pa.float64()), ("lon", pa.float64()), ("capacity", pa.int32()),
    ("temperature_c", pa.float64()), ("precipitation_mm", pa.float64()),
    ("weather_code", pa.int32()), ("wind_speed_kmh", pa.float64()),
    ("poll_id", pa.string()), ("replay", pa.bool_()), ("replayed_at", TS),
    ("kafka_topic", pa.string()), ("kafka_partition", pa.int32()), ("kafka_offset", pa.int64()),
    ("kafka_timestamp", TS), ("batch_id", pa.string()), ("processed_at", TS),
])

WEATHER = pa.schema([
    ("location_id", pa.string()), ("obs_time", TS), ("fetched_at", TS),
    ("temperature_c", pa.float64()), ("relative_humidity", pa.float64()),
    ("precipitation_mm", pa.float64()), ("rain_mm", pa.float64()), ("snowfall_cm", pa.float64()),
    ("weather_code", pa.int32()), ("cloud_cover", pa.float64()), ("wind_speed_kmh", pa.float64()),
    ("poll_id", pa.string()), ("kafka_topic", pa.string()), ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()), ("batch_id", pa.string()), ("processed_at", TS),
])

DLQ = pa.schema([
    ("reason", pa.string()), ("kafka_topic", pa.string()), ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()), ("kafka_timestamp", TS), ("raw_value", pa.string()),
    ("batch_id", pa.string()), ("processed_at", TS),
])

DIM_STATION = pa.schema([
    ("station_id", pa.string()), ("name", pa.string()), ("short_name", pa.string()),
    ("lat", pa.float64()), ("lon", pa.float64()), ("capacity", pa.int32()), ("region_id", pa.string()),
    ("feed_last_updated", pa.int64()), ("fetched_at", TS),
    ("kafka_partition", pa.int32()), ("kafka_offset", pa.int64()), ("snapshot_at", TS),
])

WINDOW_15M = pa.schema([
    ("station_id", pa.string()), ("as_of", TS), ("window_start", TS), ("n_obs", pa.int32()),
    ("coverage_min", pa.float64()), ("bikes_first", pa.int32()), ("bikes_last", pa.int32()),
    ("docks_first", pa.int32()), ("docks_last", pa.int32()), ("net_change", pa.int32()),
    ("rate_per_min", pa.float64()), ("docks_rate_per_min", pa.float64()),
    ("minutes_to_empty", pa.float64()), ("minutes_to_full", pa.float64()),
    ("at_risk_empty", pa.bool_()), ("at_risk_full", pa.bool_()), ("batch_id", pa.string()),
])

CURRENT_STATE = pa.schema([
    ("station_id", pa.string()), ("station_name", pa.string()), ("lat", pa.float64()),
    ("lon", pa.float64()), ("capacity", pa.int32()), ("state", pa.string()),
    ("num_bikes_available", pa.int32()), ("num_ebikes_available", pa.int32()),
    ("num_docks_available", pa.int32()), ("last_reported", pa.int64()), ("age_s", pa.int64()),
    ("observed_at", TS), ("net_change_15m", pa.int32()), ("rate_per_min", pa.float64()),
    ("minutes_to_empty", pa.float64()), ("minutes_to_full", pa.float64()),
    ("at_risk", pa.string()), ("temperature_c", pa.float64()), ("precipitation_mm", pa.float64()),
    ("as_of", TS), ("batch_id", pa.string()),
])

SYSTEM_AVAILABILITY = pa.schema([
    ("bucket_start", TS), ("as_of", TS), ("n_stations", pa.int32()), ("n_operational", pa.int32()),
    ("share_with_bike", pa.float64()), ("share_with_dock", pa.float64()),
    ("share_empty", pa.float64()), ("share_full", pa.float64()), ("share_stale", pa.float64()),
    ("share_offline", pa.float64()), ("n_at_risk_empty", pa.int32()), ("n_at_risk_full", pa.int32()),
    ("temperature_c", pa.float64()), ("precipitation_mm", pa.float64()), ("batch_id", pa.string()),
])

BATCH_LOG = pa.schema([
    ("batch_id", pa.string()), ("run_id", pa.string()), ("as_of", TS), ("planned_at", TS),
    ("published_at", TS), ("finished_at", TS),
    ("records_read", pa.int32()), ("raw_rows", pa.int32()), ("status_rows", pa.int32()),
    ("weather_rows", pa.int32()), ("dlq_rows", pa.int32()), ("duplicates_dropped", pa.int32()),
    ("repeats", pa.int32()), ("out_of_order", pa.int32()), ("stale", pa.int32()),
    ("unknown_station", pa.int32()), ("replay_rows", pa.int32()),
    ("latency_p50_s", pa.float64()), ("latency_p95_s", pa.float64()), ("latency_max_s", pa.float64()),
    ("status", pa.string()),
])


def to_table(rows: List[Dict[str, Any]], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)
