"""Object storage abstraction. Same keys locally and on S3; failures are loud (no silent fallbacks)."""
from __future__ import annotations

import io
import json
import os
import tempfile
from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig


class Storage:
    def put(self, key: str, data: bytes) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def get(self, key: str) -> bytes:  # pragma: no cover - interface
        raise NotImplementedError

    def exists(self, key: str) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    def list(self, prefix: str) -> List[str]:  # pragma: no cover - interface
        raise NotImplementedError

    def delete(self, key: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class LocalStorage(Storage):
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    def _path(self, key: str) -> str:
        path = os.path.abspath(os.path.join(self.root, key))
        if not path.startswith(self.root + os.sep):
            raise ValueError("key escapes storage root: %s" % key)
        return path

    def put(self, key: str, data: bytes) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            os.replace(tmp, path)      # atomic: readers never see a partial file
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def get(self, key: str) -> bytes:
        with open(self._path(key), "rb") as fh:
            return fh.read()

    def exists(self, key: str) -> bool:
        return os.path.isfile(self._path(key))

    def list(self, prefix: str) -> List[str]:
        # Walk only the directory the prefix points into, not the whole lake: keys are paths, so every
        # match lives under the prefix's last "/". The cost then scales with the prefix, not the lake.
        head = prefix.rsplit("/", 1)[0] if "/" in prefix else ""
        base = self._path(head) if head else self.root
        out = []
        for dirpath, _dirs, files in os.walk(base):
            for name in files:
                if name.endswith(".tmp"):
                    continue
                key = os.path.relpath(os.path.join(dirpath, name), self.root).replace(os.sep, "/")
                if key.startswith(prefix):
                    out.append(key)
        return sorted(out)

    def delete(self, key: str) -> None:
        try:
            os.unlink(self._path(key))
        except FileNotFoundError:
            pass


class S3Storage(Storage):
    def __init__(self, bucket: str, prefix: str = "", endpoint_url: Optional[str] = None, client: Any = None):
        if client is None:
            import boto3
            client = boto3.client("s3", endpoint_url=endpoint_url)
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client

    def _k(self, key: str) -> str:
        return "%s/%s" % (self.prefix, key) if self.prefix else key

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._k(key), Body=data)   # atomic per object

    def get(self, key: str) -> bytes:
        try:
            return self.client.get_object(Bucket=self.bucket, Key=self._k(key))["Body"].read()
        except self.client.exceptions.NoSuchKey as exc:
            raise FileNotFoundError(key) from exc

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._k(key))
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def list(self, prefix: str) -> List[str]:
        out = []
        strip = len(self.prefix) + 1 if self.prefix else 0
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._k(prefix)):
            for obj in page.get("Contents", []):
                out.append(obj["Key"][strip:])
        return sorted(out)

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._k(key))


def storage_from_config(cfg: PipelineConfig) -> Storage:
    if cfg.storage == "s3":
        if not cfg.s3_bucket:
            raise ValueError("CB_STORAGE=s3 requires CB_S3_BUCKET")
        return S3Storage(cfg.s3_bucket, cfg.s3_prefix, cfg.s3_endpoint_url)
    if cfg.storage == "local":
        return LocalStorage(cfg.local_root)
    raise ValueError("CB_STORAGE must be 'local' or 's3', got %r" % cfg.storage)


# ---- helpers -------------------------------------------------------------------------------

def put_json(storage: Storage, key: str, obj: Any) -> None:
    storage.put(key, json.dumps(obj, sort_keys=True, indent=1).encode("utf-8"))


def get_json(storage: Storage, key: str) -> Any:
    return json.loads(storage.get(key).decode("utf-8"))


def put_parquet(storage: Storage, key: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    storage.put(key, buf.getvalue())


def read_parquet(storage: Storage, key: str, columns: Optional[List[str]] = None) -> pa.Table:
    return pq.read_table(io.BytesIO(storage.get(key)), columns=columns)
