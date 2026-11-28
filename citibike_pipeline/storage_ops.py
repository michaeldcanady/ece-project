"""Helpers over any Storage: JSON and Parquet objects, and building the configured Storage."""
from __future__ import annotations

import io
import json
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from citibike_contract import JsonValue

from .config import PipelineConfig
from .local_storage import LocalStorage
from .s3_storage import S3Storage
from .storage import Storage


def put_json(storage: Storage, key: str, obj: JsonValue) -> None:
    storage.put(key, json.dumps(obj, sort_keys=True, indent=1).encode("utf-8"))


def get_json(storage: Storage, key: str) -> JsonValue:
    return json.loads(storage.get(key).decode("utf-8"))


def put_parquet(storage: Storage, key: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    storage.put(key, buf.getvalue())


def read_parquet(storage: Storage, key: str, columns: Optional[List[str]] = None) -> pa.Table:
    return pq.read_table(io.BytesIO(storage.get(key)), columns=columns)


def storage_from_config(cfg: PipelineConfig) -> Storage:
    if cfg.storage == "s3":
        if not cfg.s3_bucket:
            raise ValueError("CB_STORAGE=s3 requires CB_S3_BUCKET")
        return S3Storage(cfg.s3_bucket, cfg.s3_prefix, cfg.s3_endpoint_url)
    if cfg.storage == "local":
        return LocalStorage(cfg.local_root)
    raise ValueError("CB_STORAGE must be 'local' or 's3', got %r" % cfg.storage)
