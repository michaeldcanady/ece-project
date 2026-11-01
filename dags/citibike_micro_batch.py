"""Every minute: consume exact Kafka offset ranges, validate/dedup/enrich, publish gold, then commit.

plan_batch -> has_data -> process_batch -> compute_metrics -> finalize
Library code lives in citibike_pipeline (importable without Airflow). Imports are lazy so a
broken dependency shows up as a task failure, not as a DAG import error that hides the DAG.
"""
from datetime import timedelta

from _common import DAG_KWARGS, default_args
from airflow.decorators import dag, task


@dag(
    dag_id="citibike_micro_batch",
    schedule="* * * * *",
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args(retries=2, retry_delay=timedelta(seconds=10), execution_timeout=timedelta(minutes=4)),
    tags=["citibike", "kafka", "streaming"],
    **DAG_KWARGS,
)
def citibike_micro_batch():
    @task
    def plan(**context):
        from citibike_pipeline import batch, runtime
        cfg, storage = runtime.build()
        with runtime.open_reader(cfg) as reader:
            return batch.plan_batch(context["run_id"], reader, storage, cfg)

    @task.short_circuit
    def has_data(plan_out):
        return plan_out["total_messages"] > 0

    @task
    def process(plan_out):
        from citibike_pipeline import batch, runtime
        cfg, storage = runtime.build()
        with runtime.open_reader(cfg) as reader:
            return batch.process_batch(plan_out, reader, storage, cfg)

    @task
    def metrics(plan_out, stats):
        from citibike_pipeline import batch, runtime
        cfg, storage = runtime.build()
        return batch.compute_metrics(plan_out, stats, storage, cfg)

    @task
    def finalize(plan_out, stats, publish):
        from citibike_pipeline import batch, runtime
        cfg, storage = runtime.build()
        with runtime.open_reader(cfg) as reader:
            return batch.finalize_batch(plan_out, stats, publish, reader, storage, cfg)

    p = plan()
    gate = has_data(p)
    s = process(p)
    gate >> s
    m = metrics(p, s)
    finalize(p, s, m)


citibike_micro_batch()
