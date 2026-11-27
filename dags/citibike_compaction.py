"""Hourly: merge the per-batch files of completed hours (older than 2 h) into one file per hour."""
from datetime import timedelta

from _common import DAG_KWARGS, default_args
from airflow.decorators import dag, task


@dag(
    dag_id="citibike_compaction",
    schedule="20 * * * *",
    default_args=default_args(retries=1, retry_delay=timedelta(minutes=5), execution_timeout=timedelta(minutes=30)),
    tags=["citibike", "compaction"],
    **DAG_KWARGS,
)
def citibike_compaction() -> None:
    @task
    def compact():
        from citibike_pipeline import lake, runtime
        _, storage = runtime.build()
        return lake.compact_due(storage)

    compact()


citibike_compaction()
