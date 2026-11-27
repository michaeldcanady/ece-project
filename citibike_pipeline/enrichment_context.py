"""Enrichment context: which earlier lake rows Enrichment reads for a Batch, and the read itself.

Enrichment (`enrichment.py`) stays pure; this module is the one place that knows the windows it reads from the lake.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Tuple

from . import lake
from .config import PipelineConfig
from .decoded_batch import DecodedBatch
from .enrichment import PriorContext
from .storage import Storage
from .weather_reading import WEATHER_READING_COLS, WeatherReading

__all__ = ["EnrichmentContext"]

# The curated status columns `PriorContext.from_table` reads; `read_window` adds the event-time column itself.
_STATUS_CONTEXT_COLS = ["station_id", "last_reported"]
# How far before the Batch's first Station status the weather as-of join may look.
_WEATHER_LOOKBACK = timedelta(hours=3)


@dataclass(frozen=True)
class EnrichmentContext:
    """What Enrichment reads besides the Batch's own Station statuses: the prior context from earlier
    batches, and every Weather reading the as-of join may pick, including this Batch's own."""
    prior: PriorContext
    weather: Tuple[WeatherReading, ...]

    @classmethod
    def read(cls, storage: Storage, decoded: DecodedBatch, batch_id: str,
             cfg: PipelineConfig) -> EnrichmentContext:
        """Read the context for `decoded`, leaving out every row `batch_id` wrote before (a retry's own output)."""
        as_of = decoded.as_of
        status_times = [d.envelope.fetched_at for d in decoded.status]
        all_times = status_times + [d.envelope.fetched_at for d in decoded.weather] + [as_of]
        ctx_end = min(status_times) if status_times else as_of
        prior = (lake.CURATED_STATUS.read_window(storage, ctx_end - timedelta(minutes=cfg.context_minutes), ctx_end,
                                                 _STATUS_CONTEXT_COLS, exclude_batch=batch_id)
                 if decoded.status else None)
        # Weather from earlier batches can be newer than this batch's first status row (partitions drain at
        # different speeds), so read it up to the newest time seen here.
        prior_weather = lake.CURATED_WEATHER.read_window(storage, ctx_end - _WEATHER_LOOKBACK,
                                                         max(all_times) + timedelta(milliseconds=1),
                                                         WEATHER_READING_COLS, exclude_batch=batch_id)
        weather = (WeatherReading.from_table(prior_weather)
                   + [WeatherReading.from_observation(d.value) for d in decoded.weather])
        return cls(prior=PriorContext.from_table(prior), weather=tuple(weather))
