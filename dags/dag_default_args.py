"""The `default_args` every Citi Bike DAG passes to Airflow."""
from datetime import timedelta
from typing import TypedDict


class DagDefaultArgs(TypedDict):
    owner: str
    retries: int
    retry_delay: timedelta
    execution_timeout: timedelta
