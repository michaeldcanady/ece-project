"""The part of an Airflow task instance that run_poll uses, so a test can pass a stub."""
from __future__ import annotations

from typing import Protocol


class TaskInstanceLike(Protocol):
    task_id: str

    def xcom_pull(self, task_ids: str, key: str, include_prior_dates: bool) -> object: ...

    def xcom_push(self, key: str, value: object) -> None: ...
