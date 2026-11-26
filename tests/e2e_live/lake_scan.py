"""Read every Parquet file under a prefix, without the de-duplication LakeTable reads apply.

A check that asks "is any Kafka identity written twice?" must not read through a reader that hides the answer.
"""
from __future__ import annotations

from typing import List, Tuple

import pyarrow as pa

from citibike_pipeline.storage import Storage, read_parquet


def read_all(storage: Storage, prefix: str, columns: List[str]) -> pa.Table:
    keys = sorted(k for k in storage.list(prefix + "/") if k.endswith(".parquet"))
    tables = [read_parquet(storage, k, columns=columns) for k in keys]
    return pa.concat_tables(tables) if tables else pa.table({c: [] for c in columns})


def read_files(storage: Storage, prefix: str, columns: List[str]) -> List[Tuple[str, pa.Table]]:
    """(key, table) per Parquet file under a prefix, so a check can tell a compacted file from a batch file."""
    keys = sorted(k for k in storage.list(prefix + "/") if k.endswith(".parquet"))
    return [(k, read_parquet(storage, k, columns=columns)) for k in keys]
