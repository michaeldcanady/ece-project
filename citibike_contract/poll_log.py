"""The poll log payload: what a producer job records about each poll, and how a poll ended.

A typed view of the poll_log envelope's payload. The payload stays a plain dictionary on the wire; `to_payload`
and `from_payload` are the only places that spell its keys. Standard library only.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import List, Literal, Mapping, Optional, get_args

from citibike_contract.json_value import JsonObject

Outcome = Literal[
    "ok", "skipped_unchanged", "timeout", "connection_error", "http_error", "parse_error", "kafka_error",
    "discovery_error",
]
OUTCOMES = get_args(Outcome)

OUTCOME_OK: Outcome = "ok"
OUTCOME_SKIPPED: Outcome = "skipped_unchanged"
OUTCOME_TIMEOUT: Outcome = "timeout"
OUTCOME_CONNECTION: Outcome = "connection_error"
OUTCOME_HTTP: Outcome = "http_error"
OUTCOME_PARSE: Outcome = "parse_error"
OUTCOME_KAFKA: Outcome = "kafka_error"
OUTCOME_DISCOVERY: Outcome = "discovery_error"

_SUCCESS = (OUTCOME_OK, OUTCOME_SKIPPED)


def is_success(outcome: Outcome) -> bool:
    """Only an ok poll and a poll skipped because the feed was unchanged count as successes."""
    return outcome in _SUCCESS


class InvalidPollLog(ValueError):
    """The payload is not a valid poll log. `reasons` lists every failed check, as DLQ reason codes:
    bad_job, bad_outcome. The message is the codes joined by commas."""

    def __init__(self, reasons: List[str]) -> None:
        super().__init__(",".join(reasons))
        self.reasons = list(reasons)


@dataclass(frozen=True)
class PollLog:
    """Field order is the payload's key order on the wire. Only `job` and `outcome` are required; the rest
    default so that sparse captures still decode."""
    job: str
    outcome: Outcome
    error: Optional[str] = None
    started_at: Optional[str] = None
    fetched_at: Optional[str] = None
    http_status: Optional[int] = None
    http_latency_ms: Optional[int] = None
    feed_last_updated: Optional[int] = None
    feed_ttl: Optional[int] = None
    n_records: Optional[int] = None
    messages_sent: int = 0
    messages_delivered: int = 0
    messages_failed: int = 0
    messages_skipped: int = 0     # envelopes with no Kafka key, logged and not sent
    consecutive_failures: int = 0

    @property
    def ok(self) -> bool:
        return is_success(self.outcome)

    def to_payload(self) -> JsonObject:
        return asdict(self)

    @staticmethod
    def from_payload(payload: object) -> PollLog:
        """Raises InvalidPollLog when `job` is not a string or `outcome` is not one of OUTCOMES."""
        if not isinstance(payload, Mapping):
            raise InvalidPollLog(["bad_job", "bad_outcome"])
        reasons = []
        if not isinstance(payload.get("job"), str):
            reasons.append("bad_job")
        if payload.get("outcome") not in OUTCOMES:
            reasons.append("bad_outcome")
        if reasons:
            raise InvalidPollLog(reasons)
        known = {k: v for k, v in payload.items() if k in PollLog.__dataclass_fields__}
        return PollLog(**known)
