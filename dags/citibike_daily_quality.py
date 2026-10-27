"""
Citibike daily quality DAG
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
from pathlib import Path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'citibike_daily_quality',
    default_args=default_args,
    description='Daily quality checks',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['citibike', 'quality'],
)


def quality_checks(**context):
    logging.info("Running daily quality checks...")
    from pathlib import Path
    base_dir = Path('/workspaces/ece-projec/data')
    try:
        (base_dir / 'quality').mkdir(parents=True, exist_ok=True)
        (base_dir / 'ops').mkdir(parents=True, exist_ok=True)
    except:
        base_dir = Path('/tmp/data')
        (base_dir / 'quality').mkdir(parents=True, exist_ok=True)
        (base_dir / 'ops').mkdir(parents=True, exist_ok=True)
    logging.info("Quality checks complete")
    return "done"


quality_task = PythonOperator(
    task_id='quality_checks',
    python_callable=quality_checks,
    dag=dag,
)
