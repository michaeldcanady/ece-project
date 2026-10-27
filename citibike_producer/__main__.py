"""Entry point.

  citibike-producer live
  citibike-producer replay sample.jsonl.gz --speed 60
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading

from . import __version__
from .config import Config, TOPIC_INFO, TOPIC_STATUS
from .jobs import GbfsStationJob, Job, WeatherJob, run_loop
from .replay import replay
from .sink import KafkaSink
from .sources import FeedRegistry, HttpClient

log = logging.getLogger("citibike_producer")


def _pump(sink: KafkaSink, stop: threading.Event) -> None:
    """Serve delivery callbacks between flushes."""
    while not stop.is_set():
        sink.poll(0.5)


def run_live(cfg: Config, stop: threading.Event) -> None:
    sink = KafkaSink(cfg)
    # One HttpClient (requests.Session) per thread; the registry has its own and is lock-protected.
    registry = FeedRegistry(cfg, HttpClient(cfg))
    jobs: list[Job] = [
        GbfsStationJob(cfg, sink, HttpClient(cfg), registry, "station_status", "station_status",
                       TOPIC_STATUS, cfg.status_interval_s),
        GbfsStationJob(cfg, sink, HttpClient(cfg), registry, "station_information", "station_information",
                       TOPIC_INFO, cfg.info_interval_s),
        WeatherJob(cfg, sink, HttpClient(cfg)),
    ]
    threads = [threading.Thread(target=run_loop, args=(j, cfg, stop), name=j.name, daemon=True) for j in jobs]
    threads.append(threading.Thread(target=_pump, args=(sink, stop), name="pump", daemon=True))
    for t in threads:
        t.start()
    stop.wait()
    for t in threads:
        t.join(timeout=20)
    sink.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="citibike_producer")
    sub = parser.add_subparsers(dest="mode")
    sub.add_parser("live", help="poll live feeds (default)")
    rp = sub.add_parser("replay", help="replay a captured JSONL(.gz) sample")
    rp.add_argument("path")
    rp.add_argument("--speed", type=float, default=1.0, help="1=real time, 60=60x, 0=max")
    rp.add_argument("--max-gap", type=float, default=5.0, help="cap on any single sleep, seconds")
    rp.add_argument("--loop", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
    cfg = Config.from_env()
    log.info("citibike_producer %s starting, mode=%s, bootstrap=%s", __version__, args.mode or "live",
             cfg.bootstrap_servers)

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    if args.mode == "replay":
        sink = KafkaSink(cfg)
        try:
            n = replay(args.path, sink, cfg, stop, args.speed, args.max_gap, args.loop)
            log.info("replay finished: %d messages", n)
        finally:
            sink.close()
    else:
        run_live(cfg, stop)
    return 0


if __name__ == "__main__":
    sys.exit(main())
