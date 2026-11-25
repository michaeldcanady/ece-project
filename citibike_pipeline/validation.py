"""Payload validation for station information; the envelope itself is checked by the contract's decode, and the
station status and weather payloads by their parse in observations.py. Every rule returns reason codes;
nothing is silently dropped."""
from __future__ import annotations

from typing import Any, Dict, List


def _is_int(x: Any) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def _is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def validate_station_info_payload(p: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(p.get("station_id"), str) or not p.get("station_id"):
        errors.append("bad_station_id")
    if not _is_num(p.get("lat")) or not _is_num(p.get("lon")):
        errors.append("bad_coordinates")
    if "capacity" in p and p["capacity"] is not None and not _is_int(p["capacity"]):
        errors.append("bad_capacity")
    return errors
