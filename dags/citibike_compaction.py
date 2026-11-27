"""Hourly: merge the per-batch files of completed hours (older than 2 h) into one file per hour."""
from dataclasses import asdict
from datetime import timedelta
from typing import Mapping

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
    @task(multiple_outputs=False)   # one XCom value, the nested mapping, as before the typing work
    def compact() -> Mapping[str, Mapping[str, int]]:
        from citibike_pipeline import lake, runtime
        _, storage = runtime.build()
        return {name: asdict(result) for name, result in lake.compact_due(storage).items()}

    compact()


citibike_compaction()
