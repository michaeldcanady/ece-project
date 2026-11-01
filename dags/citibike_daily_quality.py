"""Daily 00:30 UTC: quality report for the previous UTC day (JSON + Markdown under quality/daily/)."""
from datetime import timedelta

from _common import DAG_KWARGS, default_args
from airflow.decorators import dag, task


@dag(
    dag_id="citibike_daily_quality",
    schedule="30 0 * * *",
    default_args=default_args(retries=1, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(hours=1)),
    tags=["citibike", "quality"],
    **DAG_KWARGS,
)
def citibike_daily_quality():
    @task
    def report(**context):
        from citibike_pipeline import runtime
        from citibike_pipeline.quality import daily_quality
        cfg, storage = runtime.build()
        day = context["data_interval_start"].date()      # run at 00:30 covers the day that just ended
        result = daily_quality(storage, cfg, day)
        return {"day": result["day_utc"], "verdict": result["verdict"]}

    report()


citibike_daily_quality()
