"""The part of a boto3 S3 client that S3Storage calls; the client itself has no type stubs."""
from __future__ import annotations

from typing import Mapping, Protocol

from .s3_body import S3Body
from .s3_exceptions import S3Exceptions
from .s3_paginator import S3Paginator


class S3Client(Protocol):
    @property
    def exceptions(self) -> S3Exceptions: ...

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None: ...

    def get_object(self, *, Bucket: str, Key: str) -> Mapping[str, S3Body]: ...

    def head_object(self, *, Bucket: str, Key: str) -> None: ...

    def delete_object(self, *, Bucket: str, Key: str) -> None: ...

    def get_paginator(self, operation_name: str) -> S3Paginator: ...
