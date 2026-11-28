from datetime import datetime, timezone

from citibike_pipeline.status import LakeHealth, StatusReport


def test_lines_print_none_for_missing_shares() -> None:
    health = LakeHealth(
        last_batch_finished_min_ago=1.5,
        data_as_of=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        stations=0,
        share_empty=None,
        share_full=None,
        share_stale=None,
        at_risk_empty=0,
        at_risk_full=0,
        batches_last_24h=3,
        dlq_rows_last_24h=0,
        latency_p50_s_median_last_hour=None,
    )
    lines = dict(StatusReport(ok=True, health=health).lines())
    assert lines["share_empty"] == "None"
    assert lines["share_full"] == "None"
    assert lines["share_stale"] == "None"
    assert lines["ok"] == "True"
    assert lines["stations"] == "0"
    assert lines["last_batch_finished_min_ago"] == "1.5"
    assert lines["data_as_of"] == "2026-01-02T03:04:05+00:00"
