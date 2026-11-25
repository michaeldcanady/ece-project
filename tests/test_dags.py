import pytest
from dag_catalog import Catalog, load


@pytest.fixture(scope="module")
def bag() -> Catalog:
    return load()


def test_dags_import_cleanly(bag):
    assert bag.import_errors == {}
    assert set(bag.dags) == {"citibike_setup", "citibike_ingest_status", "citibike_ingest_weather",
                             "citibike_station_info", "citibike_micro_batch", "citibike_compaction",
                             "citibike_daily_quality", "citibike_replay"}


def test_ingest_status_polls_every_minute_without_retries(bag):
    dag = bag.dags["citibike_ingest_status"]
    assert dag.schedule == "* * * * *"
    assert dag.tasks["poll"].retries == 0


def test_station_info_rebuilds_dimension_even_if_poll_fails(bag):
    task = bag.dags["citibike_station_info"].tasks["build_dim"]
    assert task.upstream == {"poll"}
    assert task.trigger_rule == "all_done"


def test_micro_batch_is_serial_every_minute_and_not_paused(bag):
    dag = bag.dags["citibike_micro_batch"]
    assert dag.max_active_runs == 1 and dag.catchup is False and dag.is_paused_upon_creation is False
    assert dag.schedule == "* * * * *"
    assert dag.task_order() == ["plan", "has_data", "process", "metrics", "finalize"]
    upstream = {t: dag.tasks[t].upstream for t in dag.task_order()}
    assert upstream == {"plan": set(), "has_data": {"plan"}, "process": {"plan", "has_data"},   # no data -> skipped
                        "metrics": {"plan", "process"}, "finalize": {"plan", "process", "metrics"}}


def test_all_dags_run_unattended(bag):
    for dag in bag.dags.values():
        assert dag.is_paused_upon_creation is False and dag.max_active_runs == 1 and dag.catchup is False


def test_micro_batch_tasks_follow_the_drivers_step_order(bag):
    from citibike_pipeline import batch
    order = bag.dags["citibike_micro_batch"].task_order()
    assert [t for t in order if t != "has_data"] == list(batch.STEPS)
    assert order[order.index("plan") + 1] == "has_data"                  # the gate sits right after the plan
