"""One poll, from fetch to poll log. A source supplies the fetch and the reading (PollSource); this module owns
the rest: skip-unchanged, one keyed envelope per record, delivery, the failure count and the poll log.

Scheduling and retries belong to Airflow; a poll only knows how to run once. State goes in and comes out as a
JobState, so nothing here is mutated."""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import List, Mapping, Optional, Sequence, Tuple

from citibike_contract import (
    OUTCOME_KAFKA,
    OUTCOME_OK,
    OUTCOME_PARSE,
    OUTCOME_SKIPPED,
    Envelope,
    MissingKafkaKey,
    Outcome,
    PollLog,
    is_success,
    iso_to_epoch_ms,
    kafka_key,
    parse_iso,
    utcnow,
)

from .config import TOPIC_POLL_LOG, Config
from .delivery_batch import DeliveryBatch
from .feed_read import FeedRead
from .fetch_result import FetchResult
from .job_state import JobState
from .kafka_sink import KafkaSink
from .poll_outcome import PollOutcome
from .poll_source import PollSource
from .unreadable_feed import UnreadableFeed

log = logging.getLogger(__name__)

FLUSH_TIMEOUT_S = 15.0


@dataclass(frozen=True)
class _Run:
    """What one poll knows from its start: who polls, from which state, and the fetch."""
    source: PollSource
    state: JobState
    cfg: Config
    sink: KafkaSink
    poll_id: str
    res: FetchResult


@dataclass(frozen=True)
class _Delivery:
    batch: DeliveryBatch
    skipped: int


def poll(source: PollSource, state: JobState, cfg: Config, sink: KafkaSink) -> PollOutcome:
    """Poll `source` once, publish its envelopes and the poll log, and return the outcome and the next state."""
    res = source.fetch()
    run = _Run(source, state, cfg, sink, uuid.uuid4().hex, res)
    if not res.ok:
        return _finish(run, res.outcome, error=res.error)
    try:
        read = source.read(res)
    except UnreadableFeed as exc:
        return _finish(run, OUTCOME_PARSE, error=str(exc))
    if (cfg.skip_unchanged_feed and read.feed_last_updated is not None
            and read.feed_last_updated == state.last_feed_updated):
        return _finish(run, OUTCOME_SKIPPED, read)

    keyed: List[Tuple[str, Envelope]] = []
    skipped = 0
    for payload in read.payloads:
        env = _envelope(run, read, payload)
        try:
            keyed.append((kafka_key(env), env))
        except MissingKafkaKey as exc:
            if not source.skips_keyless_envelopes:
                return _finish(run, OUTCOME_PARSE, read, _Delivery(DeliveryBatch(), len(read.payloads)), str(exc))
            log.warning("%s: skipping a station: %s", source.job, exc)
            skipped += 1
    return _finish_delivered(run, read, _publish(run, keyed, skipped))


def _envelope(run: _Run, read: FeedRead, payload: object) -> Envelope:
    return Envelope(event_type=run.source.event_type, poll_id=run.poll_id, source=run.source.source,
                    source_url=run.res.url, gbfs_version=read.gbfs_version,
                    feed_last_updated=read.feed_last_updated,
                    fetched_at=parse_iso(run.res.fetched_at or run.res.started_at),
                    producer_version=run.cfg.producer_version,
                    payload=payload if isinstance(payload, Mapping) else {})


def _publish(run: _Run, keyed: Sequence[Tuple[str, Envelope]], skipped: int) -> _Delivery:
    batch = DeliveryBatch()
    ts = iso_to_epoch_ms(run.res.fetched_at or run.res.started_at)
    for key, env in keyed:
        run.sink.send(run.source.topic, key, env, batch, ts)
    return _Delivery(batch, skipped)


def _finish_delivered(run: _Run, read: FeedRead, delivery: _Delivery) -> PollOutcome:
    remaining = run.sink.flush(FLUSH_TIMEOUT_S)
    if delivery.batch.failed == 0 and remaining == 0:
        return _finish(run, OUTCOME_OK, read, delivery)
    err = delivery.batch.last_error or "%d messages undelivered after flush" % remaining
    return _finish(run, OUTCOME_KAFKA, read, delivery, err)


def _finish(run: _Run, outcome: Outcome, read: Optional[FeedRead] = None, delivery: Optional[_Delivery] = None,
            error: Optional[str] = None) -> PollOutcome:
    """The poll's one exit: send the poll log and return the next state."""
    res, source = run.res, run.source
    ok = is_success(outcome)
    failures = 0 if ok else run.state.consecutive_failures + 1
    feed_last_updated = read.feed_last_updated if read else None
    # Mark the feed seen only after an ok, fully delivered poll.
    seen = feed_last_updated if outcome == OUTCOME_OK else run.state.last_feed_updated
    batch = delivery.batch if delivery else None
    poll_log = PollLog(
        job=source.job, outcome=outcome, error=error, started_at=res.started_at, fetched_at=res.fetched_at,
        http_status=res.http_status, http_latency_ms=res.latency_ms, feed_last_updated=feed_last_updated,
        feed_ttl=read.feed_ttl if read else None, n_records=read.n_records if read else None,
        messages_sent=batch.sent if batch else 0, messages_delivered=batch.delivered if batch else 0,
        messages_failed=batch.failed if batch else 0, messages_skipped=delivery.skipped if delivery else 0,
        consecutive_failures=failures)
    env = Envelope(event_type="poll_log", poll_id=run.poll_id, source=source.source, source_url=res.url,
                   gbfs_version=read.gbfs_version if read else None, feed_last_updated=feed_last_updated,
                   fetched_at=parse_iso(res.fetched_at) if res.fetched_at else utcnow(),
                   producer_version=run.cfg.producer_version, payload=dict(poll_log.to_payload()))
    run.sink.send(TOPIC_POLL_LOG, kafka_key(env), env)
    log.log(logging.INFO if ok else logging.WARNING, "%s outcome=%s records=%s latency_ms=%s failures=%d err=%s",
            source.job, outcome, read.n_records if read else None, res.latency_ms, failures, error)
    return PollOutcome(outcome=outcome, error=error, state=JobState(seen, failures))
