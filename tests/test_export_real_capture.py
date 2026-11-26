"""The real-capture export's window: what it keeps of each topic."""
import importlib.util
import os
import sys
from datetime import datetime, timedelta, timezone

from citibike_contract import TOPIC_INFO, TOPIC_POLL_LOG, TOPIC_STATUS, TOPIC_WEATHER

ROOT = os.path.join(os.path.dirname(__file__), "..")
_PATH = os.path.join(ROOT, "scripts", "export_real_capture.py")
_spec = importlib.util.spec_from_file_location("export_real_capture", _PATH)
export_real_capture = importlib.util.module_from_spec(_spec)
sys.modules["export_real_capture"] = export_real_capture   # a dataclass looks its module up here
_spec.loader.exec_module(export_real_capture)

END = datetime(2026, 11, 26, 12, 0, tzinfo=timezone.utc)


def line(topic, minutes_before_end):
    return export_real_capture.CapturedLine(END - timedelta(minutes=minutes_before_end), topic, b"{}")


def test_window_keeps_recent_status_and_poll_log_and_older_weather_and_all_info():
    lines = [line(TOPIC_STATUS, 10), line(TOPIC_STATUS, 50), line(TOPIC_POLL_LOG, 20), line(TOPIC_POLL_LOG, 90),
             line(TOPIC_WEATHER, 100), line(TOPIC_WEATHER, 400), line(TOPIC_INFO, 1000)]
    kept = export_real_capture.select(lines, END, minutes=45)
    assert [(k.topic, int((END - k.fetched_at).total_seconds() / 60)) for k in kept] == [
        (TOPIC_INFO, 1000), (TOPIC_WEATHER, 100), (TOPIC_POLL_LOG, 20), (TOPIC_STATUS, 10)]
