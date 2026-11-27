"""The day's dead-letter rows, counted by reason."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Mapping

from pydantic import BaseModel

from . import lake
from .hour_partitions import partition_path
from .storage import Storage
from .storage_ops import read_parquet


class DLQSection(BaseModel):
    total: int
    by_reason: Mapping[str, int]

    @staticmethod
    def read_day(storage: Storage, start: datetime) -> DLQSection:
        """Count the day's DLQ rows by reason. Streams one hour at a time to bound memory."""
        reasons: Counter = Counter()
        for h in range(24):
            for key in storage.list(partition_path(lake.DLQ, start + timedelta(hours=h)) + "/"):
                if key.endswith(".parquet"):
                    reasons.update(read_parquet(storage, key, columns=["reason"])["reason"].to_pylist())
        return DLQSection(total=sum(reasons.values()), by_reason=dict(reasons.most_common()))
