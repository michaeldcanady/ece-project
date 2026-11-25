"""Every minute: consume exact Kafka offset ranges, validate/dedup/enrich, publish gold, then commit.

plan -> has_data -> process -> metrics -> finalize (batch.STEPS, the order batch.run_batch takes)
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
            return batch.plan_batch(context["run_id"], reader, storage, cfg).to_json()

    @task.short_circuit
    def has_data(plan_out):
        from citibike_pipeline import batch
        return batch.Plan.from_json(plan_out).has_data

    @task
    def process(plan_out):
        from citibike_pipeline import batch, runtime
        cfg, storage = runtime.build()
        with runtime.open_reader(cfg) as reader:
            return batch.process_batch(batch.Plan.from_json(plan_out), reader, storage, cfg).to_json()

    @task
    def metrics(plan_out, stats):
        from citibike_pipeline import batch, runtime
        cfg, storage = runtime.build()
        return batch.compute_metrics(batch.Plan.from_json(plan_out), batch.Stats.from_json(stats),
                                     storage, cfg).to_json()

    @task
    def finalize(plan_out, stats, publish):
        from citibike_pipeline import batch, runtime
        cfg, storage = runtime.build()
        with runtime.open_reader(cfg) as reader:
            batch.finalize_batch(batch.Plan.from_json(plan_out), batch.Stats.from_json(stats),
                                 batch.Published.from_json(publish), reader, storage, cfg)

    p = plan()
    gate = has_data(p)
    s = process(p)
    gate >> s
    m = metrics(p, s)
    finalize(p, s, m)


citibike_micro_batch()
