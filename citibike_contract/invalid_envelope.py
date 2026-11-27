"""The error for bytes that are not a valid envelope."""
from __future__ import annotations

from typing import List


class InvalidEnvelope(ValueError):
    """The bytes are not a valid envelope. `reasons` lists every failed check, as DLQ reason codes:
    empty_value, invalid_json, not_object, bad_schema_version, unknown_event_type, missing_payload,
    missing_poll_id, bad_fetched_at, bad_replayed_at. The message is the codes joined by commas."""

    def __init__(self, reasons: List[str]) -> None:
        super().__init__(",".join(reasons))
        self.reasons = list(reasons)
