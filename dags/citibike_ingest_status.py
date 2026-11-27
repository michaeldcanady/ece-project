"""Every minute: poll GBFS station_status once and publish one message per station to Kafka.

No retries: the next run is a minute away, and a retry would double-poll. A failed poll is
recorded in pipeline.poll_log and shows up as a gap in the daily quality report.
"""
from datetime import timedelta
from enum import Enum

from _common import DAG_KWARGS, default_args, run_poll
from airflow.decorators import dag, task


class Job(Enum):
    STATION_STATUS = "station_status"
    STATION_INFORMATION = "station_information"
    WEATHER = "weather"


@dag(
    dag_id="citibike_ingest_status",
    schedule="* * * * *",
    dagrun_timeout=timedelta(seconds=58),
    default_args=default_args(retries=0, retry_delay=timedelta(seconds=0), execution_timeout=timedelta(seconds=55)),
    tags=["citibike", "kafka", "ingest"],
    **DAG_KWARGS,
)
def citibike_ingest_status():
    @task
    def poll(**context):
        return run_poll(Job.STATION_STATUS.value, context["ti"])

    poll()


citibike_ingest_status()
