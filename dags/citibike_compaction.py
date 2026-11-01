"""Hourly: merge per-minute files of completed hours (older than 2 h) into one file per hour."""
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
def citibike_compaction():
    @task
    def compact():
        from citibike_pipeline import runtime
        from citibike_pipeline.compaction import compact_due
        _, storage = runtime.build()
        return compact_due(storage)

    compact()


citibike_compaction()
