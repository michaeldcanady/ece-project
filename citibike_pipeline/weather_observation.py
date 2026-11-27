"""Weather observation: the payload of the weather event type, parsed once where it enters the pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

from citibike_contract import JsonObject

from .jsonnumbers import is_num
from .rejected import Rejected
from .timeutil import from_epoch


@dataclass(frozen=True)
class WeatherObservation:
    """The current weather at a location. Only the time and the temperature are checked by the parse; the
    other measures pass through as the source sent them."""
    location_id: Optional[str]
    obs_time: datetime
    temperature_c: Optional[float]
    relative_humidity: Optional[float]
    precipitation_mm: Optional[float]
    rain_mm: Optional[float]
    snowfall_cm: Optional[float]
    weather_code: Optional[int]
    cloud_cover: Optional[float]
    wind_speed_kmh: Optional[float]

    @classmethod
    def parse(cls, payload: JsonObject) -> Union[WeatherObservation, Rejected]:
        """The weather observation, or why the payload is not one."""
        current = payload.get("current")
        if not isinstance(current, dict):
            return Rejected(("missing_current",))
        errors = []
        if not is_num(current.get("time")):
            errors.append("bad_obs_time")
        temperature = current.get("temperature_2m")
        if temperature is not None and not is_num(temperature):
            errors.append("bad_temperature")
        if errors:
            return Rejected(tuple(errors))
        return cls(
            location_id=payload.get("location_id"),
            obs_time=from_epoch(current["time"]), temperature_c=temperature,
            relative_humidity=current.get("relative_humidity_2m"), precipitation_mm=current.get("precipitation"),
            rain_mm=current.get("rain"), snowfall_cm=current.get("snowfall"),
            weather_code=current.get("weather_code"), cloud_cover=current.get("cloud_cover"),
            wind_speed_kmh=current.get("wind_speed_10m"))
