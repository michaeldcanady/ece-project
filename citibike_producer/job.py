"""A polling job. Each poll produces data messages plus one poll_log record.

Scheduling and retries belong to Airflow (dags/citibike_ingest_*.py); a job only knows how to poll once.
State that must survive between polls is exported with `state()` and restored with `restore()`."""
from __future__ import annotations

import logging
from typing import Optional

from citibike_contract import (
    Envelope,
    Outcome,
    PollLog,
    is_success,
    kafka_key,
    parse_iso,
    utc_now_iso,
    utcnow,
)

from .config import TOPIC_POLL_LOG, Config
from .delivery_batch import DeliveryBatch
from .fetch_result import FetchResult
from .job_state import JobState
from .kafka_sink import KafkaSink

log = logging.getLogger(__name__)


class Job:
    name = "job"

    def __init__(self, cfg: Config, sink: KafkaSink):
        self.cfg = cfg
        self.sink = sink
        self.consecutive_failures = 0
        self.last_outcome: Optional[str] = None
        self.last_error: Optional[str] = None
        self._last_feed_updated: Optional[int] = None

    def state(self) -> JobState:
        return JobState(last_feed_updated=self._last_feed_updated, consecutive_failures=self.consecutive_failures)

    def restore(self, state: Optional[JobState]) -> None:
        state = state or JobState()
        self._last_feed_updated = state.last_feed_updated
        self.consecutive_failures = state.consecutive_failures

    def run_once(self) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    def _finish(self, poll_id: str, source: str, url: Optional[str], res: Optional[FetchResult], outcome: Outcome,
                gbfs_version: Optional[str] = None, feed_last_updated: Optional[int] = None,
                feed_ttl: Optional[int] = None, n_records: Optional[int] = None,
                batch: Optional[DeliveryBatch] = None, error: Optional[str] = None,
                messages_skipped: int = 0) -> bool:
        ok = is_success(outcome)
        self.consecutive_failures = 0 if ok else self.consecutive_failures + 1
        self.last_outcome, self.last_error = outcome, error
        poll_log = PollLog(
            job=self.name, outcome=outcome, error=error,
            started_at=res.started_at if res else utc_now_iso(),
            fetched_at=res.fetched_at if res else None,
            http_status=res.http_status if res else None,
            http_latency_ms=res.latency_ms if res else None,
            feed_last_updated=feed_last_updated, feed_ttl=feed_ttl, n_records=n_records,
            messages_sent=batch.sent if batch else 0, messages_delivered=batch.delivered if batch else 0,
            messages_failed=batch.failed if batch else 0, messages_skipped=messages_skipped,
            consecutive_failures=self.consecutive_failures)
        env = Envelope(event_type="poll_log", poll_id=poll_id, source=source, source_url=url or "",
                       gbfs_version=gbfs_version, feed_last_updated=feed_last_updated,
                       fetched_at=parse_iso(res.fetched_at) if res and res.fetched_at else utcnow(),
                       producer_version=self.cfg.producer_version, payload=dict(poll_log.to_payload()))
        self.sink.send(TOPIC_POLL_LOG, kafka_key(env), env)
        level = logging.INFO if ok else logging.WARNING
        log.log(level, "%s outcome=%s records=%s latency_ms=%s failures=%d err=%s", self.name, outcome,
                n_records, res.latency_ms if res else None, self.consecutive_failures, error)
        return ok
