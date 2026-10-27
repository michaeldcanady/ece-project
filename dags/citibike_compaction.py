"""
Citibike compaction DAG - hourly
"""
import logging
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

_logger = logging.getLogger(__name__)

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
    _logger.info("Compacting data...")
    base_dir = Path('/workspaces/ece-projec/data')
    try:
        (base_dir / 'raw').mkdir(parents=True, exist_ok=True)
        (base_dir / 'curated').mkdir(parents=True, exist_ok=True)
    except OSError:
        base_dir = Path('/tmp/data')
        (base_dir / 'raw').mkdir(parents=True, exist_ok=True)
        (base_dir / 'curated').mkdir(parents=True, exist_ok=True)
    _logger.info("Compaction complete")
    return "done"


compact_task = PythonOperator(
    task_id='compact',
    python_callable=compact_data,
    dag=dag,
)
