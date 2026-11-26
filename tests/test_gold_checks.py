"""The gold window rules: a row the pipeline's own metrics produce is consistent; each broken rule is named."""
from dataclasses import replace

from citibike_pipeline.config import PipelineConfig
from citibike_pipeline.metrics import Obs, window_metrics
from tests.e2e_live.gold_checks import window_row_problems

CFG = PipelineConfig()
AS_OF = 10_000.0


def row(bikes_first, bikes_last, docks_first, docks_last, minutes=14):
    obs = [Obs(AS_OF - minutes * 60, bikes_first, docks_first), Obs(AS_OF, bikes_last, docks_last)]
    r = window_metrics(obs, AS_OF, CFG)
    r.pop("window_start")
    return r


def test_rows_from_the_pipelines_own_metrics_are_consistent():
    for args in [(10, 4, 5, 11), (3, 0, 12, 15), (5, 5, 5, 5), (2, 9, 20, 13), (6, 3, 9, 0)]:
        assert window_row_problems(row(*args), CFG) == []


def test_too_little_coverage_has_no_rate_and_is_consistent():
    assert window_row_problems(row(10, 0, 5, 15, minutes=5), CFG) == []


def test_each_broken_rule_is_named():
    good = row(10, 4, 5, 11)
    assert window_row_problems({**good, "net_change": 0}, CFG) == ["net_change is not bikes_last - bikes_first"]
    assert "rate_per_min is not net change over coverage" in window_row_problems({**good, "rate_per_min": 9.0}, CFG)
    assert "minutes_to_empty does not follow from bikes_last and the rate" in window_row_problems(
        {**good, "minutes_to_empty": 1.0}, CFG)
    flagged = {**good, "at_risk_empty": False}
    assert window_row_problems(flagged, CFG) == ["at-risk flags do not follow the horizon and noise-floor rule"]
    assert window_row_problems(row(10, 4, 5, 11), replace(CFG, risk_horizon_minutes=1.0)) != []
