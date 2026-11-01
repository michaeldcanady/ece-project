"""Envelope and payload validation. Every rule returns reason codes; nothing is silently dropped."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from citibike_contract import KNOWN_EVENT_TYPES, SCHEMA_VERSION

from .timeutil import parse_iso

BOOL_LIKE = (0, 1, True, False)


def _is_int(x: Any) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def _is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def parse_envelope(value: Optional[bytes]) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    if not value:
        return None, ["empty_value"]
    try:
        env = json.loads(value.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None, ["invalid_json"]
    if not isinstance(env, dict):
        return None, ["not_object"]
    errors: List[str] = []
    if env.get("schema_version") != SCHEMA_VERSION:
        errors.append("bad_schema_version")
    if env.get("event_type") not in KNOWN_EVENT_TYPES:
        errors.append("unknown_event_type")
    if not isinstance(env.get("payload"), dict):
        errors.append("missing_payload")
    if not env.get("poll_id"):
        errors.append("missing_poll_id")
    try:
        parse_iso(env["fetched_at"])
    except (KeyError, TypeError, ValueError):
        errors.append("bad_fetched_at")
    return (env if not errors else None), errors


def validate_status_payload(p: Dict[str, Any], fetched_s: float, future_skew_s: int) -> List[str]:
    errors: List[str] = []
    sid = p.get("station_id")
    if not isinstance(sid, str) or not sid:
        errors.append("bad_station_id")
    for field in ("num_bikes_available", "num_docks_available"):
        if not _is_int(p.get(field)) or p[field] < 0:
            errors.append("bad_" + field)
    for field in ("num_ebikes_available", "num_bikes_disabled", "num_docks_disabled"):
        if field in p and (not _is_int(p[field]) or p[field] < 0):
            errors.append("bad_" + field)
    for field in ("is_installed", "is_renting", "is_returning"):
        if field in p and p[field] not in BOOL_LIKE:
            errors.append("bad_" + field)
    lr = p.get("last_reported")
    if lr is None:
        errors.append("missing_last_reported")
    elif not _is_int(lr) or lr < 0:
        errors.append("bad_last_reported")
    elif lr > fetched_s + future_skew_s:
        errors.append("last_reported_in_future")
    return errors


def validate_weather_payload(p: Dict[str, Any]) -> List[str]:
    cur = p.get("current")
    if not isinstance(cur, dict):
        return ["missing_current"]
    errors: List[str] = []
    if not _is_num(cur.get("time")):
        errors.append("bad_obs_time")
    if "temperature_2m" in cur and cur["temperature_2m"] is not None and not _is_num(cur["temperature_2m"]):
        errors.append("bad_temperature")
    return errors


def validate_poll_log_payload(p: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(p.get("job"), str):
        errors.append("bad_job")
    if not isinstance(p.get("outcome"), str):
        errors.append("bad_outcome")
    return errors


def validate_station_info_payload(p: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(p.get("station_id"), str) or not p.get("station_id"):
        errors.append("bad_station_id")
    if not _is_num(p.get("lat")) or not _is_num(p.get("lon")):
        errors.append("bad_coordinates")
    if "capacity" in p and p["capacity"] is not None and not _is_int(p["capacity"]):
        errors.append("bad_capacity")
    return errors

