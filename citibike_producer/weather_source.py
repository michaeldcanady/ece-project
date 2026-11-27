"""The Open-Meteo request."""
from __future__ import annotations

from .config import Config
from .fetch_result import FetchResult
from .http_client import HttpClient, QueryParams

WEATHER_VARIABLES = (
    "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,"
    "rain,snowfall,weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m"
)


def fetch_weather(cfg: Config, client: HttpClient) -> FetchResult:
    params: QueryParams = {
        "latitude": cfg.weather_lat,
        "longitude": cfg.weather_lon,
        "current": WEATHER_VARIABLES,
        "timeformat": "unixtime",   # unambiguous UTC epoch seconds
        "timezone": "UTC",
    }
    return client.get_json(cfg.open_meteo_url, params=params)
