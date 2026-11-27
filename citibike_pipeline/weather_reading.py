from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import pyarrow as pa

from .weather_observation import WeatherObservation

# The curated weather columns the as-of join reads; `WeatherReading.from_table` reads exactly these.
WEATHER_READING_COLS = ["obs_time", "temperature_c", "precipitation_mm", "weather_code", "wind_speed_kmh"]

@dataclass(frozen=True)
class WeatherReading:
    """The weather at one observation time, as far as a station row reads it."""
    obs_time: datetime
    temperature_c: Optional[float]
    precipitation_mm: Optional[float]
    weather_code: Optional[int]
    wind_speed_kmh: Optional[float]

    @classmethod
    def from_observation(cls, obs: WeatherObservation) -> WeatherReading:
        return cls(obs.obs_time, obs.temperature_c, obs.precipitation_mm, obs.weather_code, obs.wind_speed_kmh)

    @staticmethod
    def from_table(table: pa.Table) -> List[WeatherReading]:
        cols = {c: table[c].to_pylist() for c in WEATHER_READING_COLS}
        return [WeatherReading(**{c: cols[c][i] for c in WEATHER_READING_COLS}) for i in range(table.num_rows)]
