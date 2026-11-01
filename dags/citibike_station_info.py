"""Hourly: poll GBFS station_information into Kafka, then rebuild the station dimension from the
compacted topic. The rebuild runs even if the poll failed: the topic still holds the last good snapshot."""
from datetime import timedelta

from _common import DAG_KWARGS, default_args, run_poll
from airflow.decorators import dag, task


@dag(
    dag_id="citibike_station_info",
    schedule="5 * * * *",
    default_args=default_args(retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=15)),
    tags=["citibike", "station-info", "ingest"],
    **DAG_KWARGS,
)
def citibike_station_info():
    @task
    def poll(**context):
        return run_poll("station_information", context)

    @task(trigger_rule="all_done")
    def build_dim():
        from citibike_pipeline import runtime
        from citibike_pipeline.station_info import build_dim_station
        cfg, storage = runtime.build()
        with runtime.open_reader(cfg) as reader:
            return build_dim_station(reader, storage, cfg)

    poll() >> build_dim()


citibike_station_info()
