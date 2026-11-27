"""What one job's poll logs say about a UTC day: outcomes, latency and, for station status, completeness and gaps."""
from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime
from typing import List, Mapping, MutableMapping, Optional, Tuple, Union

from pydantic import BaseModel, SerializerFunctionWrapHandler, model_serializer

from citibike_contract import InvalidPollLog, PollLog

from . import lake
from .config import PipelineConfig
from .percentile import pct
from .poll_gap import PollGap
from .storage import Storage
from .timeutil import epoch, from_epoch, iso

log = logging.getLogger(__name__)

_GAP_FIELDS = ("expected_polls", "completeness", "gaps_over_threshold", "gap_seconds_total", "largest_gaps")


class PollSection(BaseModel):
    polls_logged: int
    outcomes: Mapping[str, int]
    http_latency_ms_p50: Optional[int]
    http_latency_ms_p95: Optional[int]
    # Only the station-status job is checked for completeness; the other jobs leave these out of the JSON.
    expected_polls: Optional[int] = None
    completeness: Optional[float] = None
    gaps_over_threshold: Optional[int] = None
    gap_seconds_total: Optional[Union[int, float]] = None     # 0 (not 0.0) with no gaps, as on the wire
    largest_gaps: Optional[List[PollGap]] = None

    @model_serializer(mode="wrap")
    def _without_unmeasured_gaps(self, handler: SerializerFunctionWrapHandler) -> Mapping[str, object]:
        data = handler(self)
        if self.expected_polls is not None:
            return data
        return {k: v for k, v in data.items() if k not in _GAP_FIELDS}

    @staticmethod
    def read_day(storage: Storage, cfg: PipelineConfig, start: datetime, end: datetime) -> Mapping[str, PollSection]:
        """One section per job that logged a poll in [start, end)."""
        t = lake.RAW_POLL_LOG.read_window(storage, start, end, ["fetched_at", "payload_json"])
        by_job: MutableMapping[str, List[Tuple[float, PollLog]]] = {}
        for fetched, payload in zip(t["fetched_at"].to_pylist(), t["payload_json"].to_pylist()):
            try:
                p = PollLog.from_payload(json.loads(payload))
            except (InvalidPollLog, ValueError) as exc:
                log.warning("skipping an undecodable poll log at %s: %s", iso(fetched), exc)
                continue
            by_job.setdefault(p.job, []).append((epoch(fetched), p))

        out: MutableMapping[str, PollSection] = {}
        for job, items in sorted(by_job.items()):
            items.sort(key=lambda item: item[0])
            outcomes: Counter[str] = Counter(p.outcome for _, p in items)
            lat = sorted(p.http_latency_ms for _, p in items if p.http_latency_ms is not None)
            p50, p95 = pct(lat, 0.5), pct(lat, 0.95)
            if job != "station_status":
                out[job] = PollSection(polls_logged=len(items), outcomes=dict(outcomes),
                                       http_latency_ms_p50=p50, http_latency_ms_p95=p95)
                continue
            expected = int(86400 / cfg.status_interval_s)
            times = [t_ for t_, _ in items]
            gaps = [PollGap(from_=from_epoch(a).isoformat(timespec="milliseconds"),
                            to=from_epoch(b).isoformat(timespec="milliseconds"), seconds=round(b - a, 1))
                    for a, b in zip(times, times[1:]) if b - a > cfg.gap_threshold_s]
            out[job] = PollSection(
                polls_logged=len(items), outcomes=dict(outcomes), http_latency_ms_p50=p50, http_latency_ms_p95=p95,
                expected_polls=expected, completeness=round(len(items) / float(expected), 4),
                gaps_over_threshold=len(gaps), gap_seconds_total=round(sum(g.seconds for g in gaps), 1),
                largest_gaps=sorted(gaps, key=lambda g: -g.seconds)[:10])
        return out
