"""Deterministic synthetic capture, produced by the *real* producer jobs.

Only the HTTP layer and the clock are simulated, so envelopes and poll_log records match the live
contract exactly. Useful for tests, replay demos, and offline development; not for results.

    python -m citibike_producer.synthetic sample.jsonl.gz --minutes 45 --stations 40
"""
from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from citibike_contract import iso

from .config import TOPIC_INFO, TOPIC_STATUS, Config
from .jobs import GbfsStationJob, WeatherJob
from .sink import KafkaSink
from .sources import FetchResult

URL_STATUS = "https://gbfs.citibikenyc.com/gbfs/2.3/en/station_status.json"
URL_INFO = "https://gbfs.citibikenyc.com/gbfs/2.3/en/station_information.json"
STATUS_INTERVAL_S = 60     # the citibike_ingest_status DAG schedule
WEATHER_INTERVAL_S = 900   # the citibike_ingest_weather DAG schedule


class _NullProducer:
    """Accepts messages; fires delivery callbacks on flush like a healthy broker."""

    def __init__(self) -> None:
        self._cbs: List[Any] = []

    def produce(self, topic, key=None, value=None, on_delivery=None, timestamp=None):
        if on_delivery:
            self._cbs.append(on_delivery)

    def poll(self, t=0):
        return 0

    def flush(self, t=0):
        for cb in self._cbs:
            cb(None, None)
        self._cbs = []
        return 0


class _Sim:
    """Scenario: 0 depletes, 1 fills, 2 stays empty, 3 is stale, 4 is offline, 5 has a bad record."""

    def __init__(self, n: int, seed: int):
        self.n = n
        self.rng = random.Random(seed)
        self.cap = [self.rng.choice([24, 30, 36]) for _ in range(n)]
        self.bikes = [self.rng.randint(5, c - 5) for c in self.cap]
        self.cap[0], self.cap[1], self.cap[2] = 30, 36, 30
        self.bikes[0], self.bikes[1], self.bikes[2] = 18, 14, 0       # st-001 fills to 2 free docks by minute 40

    def sid(self, i: int) -> str:
        return "st-%03d" % i

    def step(self, elapsed_s: int) -> None:
        for i in range(3, self.n):
            if self.rng.random() < 0.3:
                self.bikes[i] = max(0, min(self.cap[i], self.bikes[i] + self.rng.choice([-1, 1])))
        if elapsed_s % 120 == 0:            # one bike per two minutes
            self.bikes[0] = max(0, self.bikes[0] - 1)
            self.bikes[1] = min(self.cap[1], self.bikes[1] + 1)

    def stations(self, t: datetime, t0: datetime) -> List[Dict[str, Any]]:
        out = []
        for i in range(self.n):
            b, c = self.bikes[i], self.cap[i]
            out.append({
                "station_id": self.sid(i), "num_bikes_available": b, "num_ebikes_available": min(b, 2),
                "num_docks_available": c - b, "num_bikes_disabled": 0, "num_docks_disabled": 0,
                "is_installed": 1, "is_renting": 0 if i == 4 else 1, "is_returning": 1,
                "last_reported": int((t0 - timedelta(hours=3)).timestamp()) if i == 3 else int(t.timestamp()),
            })
        out[5]["num_bikes_available"] = -1        # fails validation downstream (-> DLQ)
        out.append({"station_id": "st-unknown", "num_bikes_available": 3, "num_docks_available": 7,
                    "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": int(t.timestamp())})
        return out

    def info(self) -> List[Dict[str, Any]]:
        return [{"station_id": self.sid(i), "name": "Synthetic St %d" % i, "short_name": "S%d" % i,
                 "lat": 40.70 + i * 0.001, "lon": -74.00 + i * 0.001, "capacity": self.cap[i], "region_id": "71"}
                for i in range(self.n)]


class _Clock:
    def __init__(self, now: datetime):
        self.now = now


class _Client:
    def __init__(self, clock: _Clock, body_fn):
        self.clock, self.body_fn = clock, body_fn

    def get_json(self, url, params=None) -> FetchResult:
        t = self.clock.now
        return FetchResult("ok", url, iso(t - timedelta(milliseconds=180)), iso(t), 200, 180, self.body_fn(t))


class _Registry:
    version = "2.3"

    def url_for(self, name):
        return {"station_status": URL_STATUS, "station_information": URL_INFO}[name]

    def invalidate(self):
        pass


def generate(path: str, stations: int = 40, minutes: int = 45, start: Optional[datetime] = None,
             seed: int = 7) -> str:
    """Write a gzip JSONL capture and return its path. `start` defaults to `minutes` ago."""
    if stations < 8:
        raise ValueError("the scenario needs at least 8 stations (indices 0-5 carry fixed behaviours)")
    t0 = start or (datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=minutes))
    if os.path.exists(path):
        os.remove(path)          # the sink's tee appends; a capture always starts from an empty file
    sim, clock = _Sim(stations, seed), _Clock(t0)
    cfg = Config(tee_path=path, producer_version="synthetic")
    sink = KafkaSink(cfg, producer=_NullProducer())
    reg = _Registry()

    status = GbfsStationJob(cfg, sink, _Client(clock, lambda t: {
        "last_updated": int(t.timestamp()), "ttl": 5, "version": "2.3",
        "data": {"stations": sim.stations(t, t0)}}), reg, "station_status", "station_status", TOPIC_STATUS)
    info = GbfsStationJob(cfg, sink, _Client(clock, lambda t: {
        "last_updated": int(t.timestamp()), "ttl": 60, "version": "2.3",
        "data": {"stations": sim.info()}}), reg, "station_information", "station_information", TOPIC_INFO)

    def weather_body(t: datetime) -> Dict[str, Any]:
        obs = int(t.timestamp()) // 900 * 900
        return {"latitude": 40.75, "longitude": -73.98, "current_units": {"temperature_2m": "°C"},
                "current": {"time": obs, "interval": 900, "temperature_2m": 12.0 + (obs // 900 % 8) * 0.5,
                            "relative_humidity_2m": 60, "precipitation": 0.0, "rain": 0.0, "snowfall": 0.0,
                            "weather_code": 3, "cloud_cover": 90, "wind_speed_10m": 11.5}}

    weather = WeatherJob(cfg, sink, _Client(clock, weather_body))
    info.run_once()
    weather.run_once()
    for k in range(minutes * 60 // STATUS_INTERVAL_S):       # one station_status poll per DAG run
        elapsed = k * STATUS_INTERVAL_S
        clock.now = t0 + timedelta(seconds=elapsed)
        sim.step(elapsed)
        status.run_once()
        if elapsed and elapsed % WEATHER_INTERVAL_S == 0:
            weather.run_once()
    sink.close()
    return path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("path", nargs="?", default="data/sample/sample_capture.jsonl.gz")
    ap.add_argument("--stations", type=int, default=40)
    ap.add_argument("--minutes", type=int, default=45)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--start", default=None, help="ISO-8601 UTC start time (default: now minus --minutes)")
    args = ap.parse_args(argv)
    os.makedirs(os.path.dirname(os.path.abspath(args.path)), exist_ok=True)
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")) if args.start else None
    generate(args.path, args.stations, args.minutes, start=start, seed=args.seed)
    print("wrote %s" % args.path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
