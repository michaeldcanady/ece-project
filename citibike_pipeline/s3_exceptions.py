"""The exceptions an S3 client exposes."""
from __future__ import annotations

from typing import Protocol, Type


class S3Exceptions(Protocol):
    NoSuchKey: Type[Exception]
