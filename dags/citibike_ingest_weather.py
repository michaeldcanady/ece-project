"""Every 15 minutes: poll Open-Meteo current conditions once and publish to Kafka."""
from datetime import timedelta

from _common import DAG_KWARGS, default_args, run_poll
from airflow.decorators import dag, task


@dag(
    dag_id="citibike_ingest_weather",
    schedule="*/15 * * * *",
    default_args=default_args(retries=2, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(minutes=2)),
    tags=["citibike", "kafka", "ingest"],
    **DAG_KWARGS,
)
def citibike_ingest_weather():
    @task
    def poll(**context):
        return run_poll("weather", context)

    poll()


citibike_ingest_weather()
