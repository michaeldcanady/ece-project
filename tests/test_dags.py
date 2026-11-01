import os

import pytest

pytest.importorskip("airflow")

DAGS = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def bag():
    from airflow.models import DagBag
    return DagBag(DAGS, include_examples=False)


def test_dags_import_cleanly(bag):
    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {"citibike_setup", "citibike_ingest_status", "citibike_ingest_weather",
                                "citibike_station_info", "citibike_micro_batch", "citibike_compaction",
                                "citibike_daily_quality", "citibike_replay"}


def test_ingest_status_polls_every_minute_without_retries(bag):
    dag = bag.dags["citibike_ingest_status"]
    assert str(dag.timetable.summary) == "* * * * *"
    assert dag.get_task("poll").retries == 0


def test_station_info_rebuilds_dimension_even_if_poll_fails(bag):
    dag = bag.dags["citibike_station_info"]
    assert dag.get_task("build_dim").upstream_task_ids == {"poll"}
    assert dag.get_task("build_dim").trigger_rule == "all_done"


def test_micro_batch_is_serial_every_minute_and_not_paused(bag):
    dag = bag.dags["citibike_micro_batch"]
    assert dag.max_active_runs == 1 and dag.catchup is False and dag.is_paused_upon_creation is False
    assert str(dag.timetable.summary) == "* * * * *"
    assert [t.task_id for t in dag.topological_sort()] == ["plan", "has_data", "process", "metrics", "finalize"]
    assert "has_data" in dag.get_task("process").upstream_task_ids       # no data -> downstream skipped


def test_all_dags_run_unattended(bag):
    for dag in bag.dags.values():
        assert dag.is_paused_upon_creation is False and dag.max_active_runs == 1 and dag.catchup is False
