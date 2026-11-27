"""Storage on S3 or an S3-compatible endpoint."""
from __future__ import annotations

from typing import List, Optional

from .s3_client import S3Client


class S3Storage:
    def __init__(self, bucket: str, prefix: str = "", endpoint_url: Optional[str] = None,
                 client: Optional[S3Client] = None):
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
