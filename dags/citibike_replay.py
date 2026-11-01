"""Manual: replay a capture (one file, or a CB_CAPTURE_DIR directory) into Kafka.

Trigger with config, e.g. {"path": "/opt/airflow/data/capture", "speed": 60}. Replay into a clean
lake: serving time never moves backwards, so replaying older data over live data is ignored.
"""
from datetime import timedelta

from _common import DAG_KWARGS, default_args
from airflow.decorators import dag, task
from airflow.models.param import Param

SAMPLE = "/opt/airflow/project/data/sample/sample_capture.jsonl.gz"


@dag(
    dag_id="citibike_replay",
    schedule=None,
    params={
        "path": Param(SAMPLE, type="string", description="capture file or directory inside the Airflow container"),
        "speed": Param(60.0, type="number", minimum=0, description="1 = real time, 60 = 60x, 0 = as fast as possible"),
    },
    default_args=default_args(retries=0, retry_delay=timedelta(seconds=0), execution_timeout=timedelta(hours=2)),
    tags=["citibike", "kafka", "replay"],
    **DAG_KWARGS,
)
def citibike_replay():
    @task
    def replay(**context):
        from citibike_producer.tasks import replay_capture
        params = context["params"]
        return {"messages": replay_capture(params["path"], speed=float(params["speed"]))}

    replay()


citibike_replay()
