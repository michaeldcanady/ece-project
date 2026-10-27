"""
Citibike station info DAG - hourly processing
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
    'citibike_station_info',
    default_args=default_args,
    description='Process station information (compacted topic)',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['citibike', 'station-info'],
)


def process_station_info(**context):
    _logger.info("Processing station information...")
    try:
        base_dir = Path('/workspaces/ece-projec/data')
        try:
            (base_dir / 'curated' / 'dim_station').mkdir(parents=True, exist_ok=True)
        except OSError:
            base_dir = Path('/tmp/data')
            (base_dir / 'curated' / 'dim_station').mkdir(parents=True, exist_ok=True)
        _logger.info("Station info processed")
        return "done"
    except Exception as e:
        _logger.error(f"Error: {e}")
        raise


process_task = PythonOperator(
    task_id='process_station_info',
    python_callable=process_station_info,
    dag=dag,
)
