"""HTTP access to GBFS and Open-Meteo. Never raises: failures come back as FetchResult."""
from __future__ import annotations

import time
from typing import Mapping, Optional, Union

import requests

from citibike_contract import (
    OUTCOME_CONNECTION,
    OUTCOME_HTTP,
    OUTCOME_OK,
    OUTCOME_PARSE,
    OUTCOME_TIMEOUT,
    utc_now_iso,
)

from .config import Config
from .fetch_result import FetchResult

QueryParams = Mapping[str, Union[str, float]]


class HttpClient:
    def __init__(self, cfg: Config, session: Optional[requests.Session] = None):
        self._timeout = (cfg.http_connect_timeout_s, cfg.http_read_timeout_s)
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": cfg.user_agent, "Accept": "application/json"})

    def get_json(self, url: str, params: Optional[QueryParams] = None) -> FetchResult:
        started_at = utc_now_iso()
        t0 = time.monotonic()
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
        except requests.Timeout as exc:
            return FetchResult(OUTCOME_TIMEOUT, url, started_at, error=str(exc)[:300])
        except requests.RequestException as exc:
            return FetchResult(OUTCOME_CONNECTION, url, started_at, error=str(exc)[:300])
        fetched_at = utc_now_iso()
        latency_ms = int((time.monotonic() - t0) * 1000)
        if resp.status_code != 200:
            return FetchResult(OUTCOME_HTTP, url, started_at, fetched_at, resp.status_code,
                               latency_ms, error="HTTP %d" % resp.status_code)
        try:
            body = resp.json()
        except ValueError as exc:
            return FetchResult(OUTCOME_PARSE, url, started_at, fetched_at, resp.status_code,
                               latency_ms, error="invalid JSON: %s" % str(exc)[:200])
        if not isinstance(body, dict):
            return FetchResult(OUTCOME_PARSE, url, started_at, fetched_at, resp.status_code,
                               latency_ms, error="JSON root is not an object")
        return FetchResult(OUTCOME_OK, url, started_at, fetched_at, resp.status_code, latency_ms, body)
