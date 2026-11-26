"""Batch decode: what one Batch's Kafka records become.

`DecodedBatch.decode` hides the decode protocol: envelope decode, the topic/event-type check, the per-event-type
parse, reason-code joining, raw-row provenance and the Batch's event-time horizon (`as_of`).
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import DefaultDict, List, Mapping, Sequence, Tuple, Union

from citibike_contract import EVENT_TYPE_BY_TOPIC, InvalidEnvelope, InvalidPollLog, PollLog, decode

from .config import PipelineConfig
from .dead_letter_row import DeadLetterRow
from .kafka_io import Record
from .observations import Delivered, JsonObject, Rejected, StationStatus, WeatherObservation
from .raw_row import RawRow
from .timeutil import epoch, from_epoch

_Parsed = Union[StationStatus, WeatherObservation, Rejected, None]


@dataclass(frozen=True)
class DecodedBatch:
    """A Batch's records decoded: raw rows, dead-letter rows and the valid observations, in record order."""
    raw_rows: Tuple[RawRow, ...]
    dead_letters: Tuple[DeadLetterRow, ...]
    status: Tuple[Delivered[StationStatus], ...]
    weather: Tuple[Delivered[WeatherObservation], ...]
    records_read: int
    as_of: datetime                  # the Batch's event-time horizon

    @classmethod
    def decode(cls, records: Sequence[Record], batch_id: str, processed_at: datetime,
               cfg: PipelineConfig) -> DecodedBatch:
        raw_rows: List[RawRow] = []
        dead_letters: List[DeadLetterRow] = []
        status: List[Delivered[StationStatus]] = []
        weather: List[Delivered[WeatherObservation]] = []
        for rec in records:
            try:
                env = decode(rec.value)
            except InvalidEnvelope as e:
                dead_letters.append(DeadLetterRow.from_record(rec, e.reasons, batch_id, processed_at))
                continue
            if EVENT_TYPE_BY_TOPIC.get(rec.topic) != env.event_type:
                dead_letters.append(DeadLetterRow.from_record(rec, ("event_type_topic_mismatch",), batch_id,
                                                              processed_at))
                continue
            parsed, reasons = _parse(env.event_type, env.payload, epoch(env.fetched_at), cfg)
            raw_rows.append(RawRow.from_delivery(rec, env, batch_id, processed_at))
            if reasons:
                dead_letters.append(DeadLetterRow.from_record(rec, reasons, batch_id, processed_at))
            elif isinstance(parsed, StationStatus):
                status.append(Delivered(rec, env, parsed))
            elif isinstance(parsed, WeatherObservation):
                weather.append(Delivered(rec, env, parsed))
        return cls(raw_rows=tuple(raw_rows), dead_letters=tuple(dead_letters), status=tuple(status),
                   weather=tuple(weather), records_read=len(records),
                   as_of=_as_of(status, weather, raw_rows, records, processed_at))

    def raw_rows_by_event_type(self) -> Mapping[str, Sequence[RawRow]]:
        """The raw rows grouped by event type, each group in record order."""
        groups: DefaultDict[str, List[RawRow]] = defaultdict(list)
        for r in self.raw_rows:
            groups[r.event_type].append(r)
        return groups


def _parse(event_type: str, payload: JsonObject, fetched_s: float,
           cfg: PipelineConfig) -> Tuple[_Parsed, Tuple[str, ...]]:
    """The payload parsed for its event type, and the reasons it was rejected (empty when valid)."""
    if event_type == "station_status":
        status = StationStatus.parse(payload, fetched_s, cfg.future_skew_s)
        return status, status.reasons if isinstance(status, Rejected) else ()
    if event_type == "weather_current":
        weather = WeatherObservation.parse(payload)
        return weather, weather.reasons if isinstance(weather, Rejected) else ()
    try:
        PollLog.from_payload(payload)
    except InvalidPollLog as exc:
        return None, tuple(exc.reasons)
    return None, ()


def _as_of(status: Sequence[Delivered[StationStatus]], weather: Sequence[Delivered[WeatherObservation]],
           raw_rows: Sequence[RawRow], records: Sequence[Record], processed_at: datetime) -> datetime:
    """Event-time horizon of the Batch. Status observations define it: weather and poll_log sit on their own
    partitions and can be ahead of the status partitions during catch-up or replay. Then weather, then any raw
    row, then the newest Kafka timestamp, then processed_at."""
    if status:
        return max(d.envelope.fetched_at for d in status)
    if weather:
        return max(d.envelope.fetched_at for d in weather)
    if raw_rows:
        return max(r.fetched_at for r in raw_rows)
    kafka_ms = [r.timestamp_ms for r in records if r.timestamp_ms]
    return from_epoch(max(kafka_ms) / 1000.0) if kafka_ms else processed_at
