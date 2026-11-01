"""Event envelope: provenance travels with every message."""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from citibike_contract import SCHEMA_VERSION, iso_to_epoch_ms, utc_now_iso  # noqa: F401


def make_envelope(
    event_type: str,
    source: str,
    source_url: str,
    gbfs_version: Optional[str],
    feed_last_updated: Optional[int],
    poll_id: str,
    fetched_at: str,
    payload: Dict[str, Any],
    producer_version: str,
) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": event_type,
        "poll_id": poll_id,
        "source": source,
        "source_url": source_url,
        "gbfs_version": gbfs_version,
        "feed_last_updated": feed_last_updated,  # source timestamp (epoch seconds)
        "fetched_at": fetched_at,                # ingest timestamp (UTC, producer clock)
        "producer_version": producer_version,
        "payload": payload,
    }


def encode(envelope: Dict[str, Any]) -> bytes:
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
