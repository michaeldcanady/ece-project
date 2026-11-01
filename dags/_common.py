"""Settings shared by every Citi Bike DAG. Standard library and pendulum only, so importing this
file can never fail because of a pipeline dependency."""
from datetime import timedelta

import pendulum

# Every DAG runs unattended, one run at a time, with no backfill after downtime.
DAG_KWARGS = {
    "start_date": pendulum.datetime(2026, 10, 1, tz="UTC"),
    "catchup": False,
    "max_active_runs": 1,
    "is_paused_upon_creation": False,
}


def default_args(retries: int, retry_delay: timedelta, execution_timeout: timedelta) -> dict:
    return {"owner": "airflow", "retries": retries, "retry_delay": retry_delay,
            "execution_timeout": execution_timeout}


STATE_KEY = "producer_state"


def run_poll(job: str, context: dict) -> dict:
    """Poll one source once, carrying the producer's state between runs in XCom.

    The state is saved before a failed poll fails the task, so the failure count and the
    discovered feed URLs survive into the next run. The failure itself is already in poll_log.
    """
    from citibike_producer.tasks import poll_once

    ti = context["ti"]
    prior = ti.xcom_pull(task_ids=ti.task_id, key=STATE_KEY, include_prior_dates=True)
    result = poll_once(job, prior)
    ti.xcom_push(key=STATE_KEY, value=result["state"])
    if not result["ok"]:
        raise RuntimeError("%s poll failed: %s (%s)" % (job, result["outcome"], result["error"]))
    return {"outcome": result["outcome"]}
