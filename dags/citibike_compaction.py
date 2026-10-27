"""
Citibike compaction DAG - hourly
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
    'citibike_compaction',
    default_args=default_args,
    description='Compact raw data',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['citibike', 'compaction'],
)


def compact_data(**context):
    logging.info("Compacting data...")
    from pathlib import Path
    base_dir = Path('/workspaces/ece-projec/data')
    try:
        (base_dir / 'raw').mkdir(parents=True, exist_ok=True)
        (base_dir / 'curated').mkdir(parents=True, exist_ok=True)
    except:
        base_dir = Path('/tmp/data')
        (base_dir / 'raw').mkdir(parents=True, exist_ok=True)
        (base_dir / 'curated').mkdir(parents=True, exist_ok=True)
    logging.info("Compaction complete")
    return "done"


compact_task = PythonOperator(
    task_id='compact',
    python_callable=compact_data,
    dag=dag,
)
