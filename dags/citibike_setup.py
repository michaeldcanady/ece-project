"""Once, at first start: create the Kafka topics (idempotent). Re-trigger it by hand after a broker reset."""
from datetime import timedelta

from _common import DAG_KWARGS, default_args
from airflow.decorators import dag, task


@dag(
    dag_id="citibike_setup",
    schedule="@once",
    default_args=default_args(retries=5, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=2)),
    tags=["citibike", "setup"],
    **DAG_KWARGS,
)
def citibike_setup():
    @task
    def create_topics():
        from citibike_producer.config import Config
        from citibike_producer.create_topics import ensure_topics
        return ensure_topics(Config.from_env().bootstrap_servers, replication=1)

    create_topics()


citibike_setup()
