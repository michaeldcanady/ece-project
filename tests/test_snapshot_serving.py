"""The pure Snapshot calculation (no lake, no Kafka) and the serving files' publish rules."""
from datetime import datetime, timedelta, timezone

from citibike_pipeline import lake, serving
from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.snapshot import StatusRow, compute_snapshot
from citibike_pipeline.storage import LocalStorage

CFG = PipelineConfig()
AS_OF = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def row(station, minutes_ago, bikes, docks, offset=0, *, installed=True, last_reported_age_s=0, **kw):
    t = AS_OF - timedelta(minutes=minutes_ago)
    return StatusRow(
        kafka_partition=0, kafka_offset=offset, station_id=station, fetched_at=t,
        last_reported=int(t.timestamp()) - last_reported_age_s, num_bikes_available=bikes,
        num_ebikes_available=0, num_docks_available=docks, is_installed=installed, is_renting=True,
        is_returning=True, station_name=station, lat=40.0, lon=-73.0, capacity=bikes + docks,
        temperature_c=kw.get("temp"), precipitation_mm=None)


def depleting(station):
    """30 bikes falling 1.5 per minute over 14 minutes: at risk of emptying."""
    return [row(station, 14 - 2 * i, 30 - i * 3, 10 + i * 3, offset=i) for i in range(8)]


def test_no_rows_gives_no_snapshot():
    assert compute_snapshot([], AS_OF, "b1", CFG) is None


def test_states_shares_and_at_risk_order():
    rows = (depleting("fast")
            + [row("empty", 1, 0, 20), row("full", 1, 20, 0), row("off", 1, 5, 5, installed=False),
               row("stale", 1, 5, 5, last_reported_age_s=3 * 3600), row("ok", 1, 10, 10)])
    snap = compute_snapshot(rows, AS_OF, "b1", CFG)
    state = {s.station_id: s for s in snap.states}
    assert {k: v.state for k, v in state.items()} == {
        "fast": "OK", "empty": "EMPTY", "full": "FULL", "off": "OFFLINE", "stale": "STALE", "ok": "OK"}
    assert state["fast"].at_risk == "EMPTY" and state["fast"].net_change_15m < 0
    assert [s.station_id for s in snap.at_risk] == ["fast"]
    a = snap.availability
    assert (a.n_stations, a.n_operational) == (6, 5)
    assert a.share_offline == 1 / 6 and a.share_empty == 1 / 6 and a.n_at_risk_empty == 1
    assert a.bucket_start == AS_OF and snap.as_of == AS_OF and all(s.batch_id == "b1" for s in snap.states)


def test_at_risk_is_most_urgent_first():
    slow = [row("slow", 14 - 2 * i, 30 - i * 2, 10, offset=i) for i in range(8)]
    snap = compute_snapshot(slow + depleting("fast"), AS_OF, "b1", CFG)
    ids = [s.station_id for s in snap.at_risk]
    assert ids == sorted(ids, key=lambda i: {"fast": 0, "slow": 1}[i])


def test_single_observation_reports_state_without_a_rate():
    snap = compute_snapshot([row("one", 1, 0, 12)], AS_OF, "b1", CFG)
    s = snap.states[0]
    assert s.state == "EMPTY" and s.rate_per_min is None and s.minutes_to_empty == 0.0 and s.at_risk is None
    assert snap.windows[0].n_obs == 1 and snap.at_risk == []


def test_serving_time_never_moves_backwards(tmp_path):
    storage = LocalStorage(str(tmp_path))
    assert serving.serving_as_of(storage, AS_OF) == AS_OF            # nothing published yet
    snap = compute_snapshot([row("a", 1, 5, 5)], AS_OF, "b1", CFG)
    serving.publish(storage, snap)
    assert serving.read_summary(storage).as_of == AS_OF
    assert serving.serving_as_of(storage, AS_OF - timedelta(minutes=5)) == AS_OF   # catch-up batch
    later = AS_OF + timedelta(minutes=5)
    assert serving.serving_as_of(storage, later) == later


def test_publish_writes_history_before_serving_files(tmp_path):
    class Recording(LocalStorage):
        puts = []

        def put(self, key, data):
            self.puts.append(key)
            super().put(key, data)

    storage = Recording(str(tmp_path))
    serving.publish(storage, compute_snapshot([row("a", 1, 5, 5)], AS_OF, "b1", CFG))
    first_serving = min(i for i, k in enumerate(storage.puts) if k.startswith("gold/serving/"))
    history = [i for i, k in enumerate(storage.puts) if k.startswith((lake.GOLD_WINDOW, lake.GOLD_AVAILABILITY))]
    assert len(history) == 2 and max(history) < first_serving
    assert set(storage.puts[first_serving:]) == {serving.AT_RISK, serving.SUMMARY, serving.CURRENT}
