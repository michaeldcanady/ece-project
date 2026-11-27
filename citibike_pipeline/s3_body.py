"""The readable body of an S3 object."""
from __future__ import annotations

from typing import Protocol


class S3Body(Protocol):
    def read(self) -> bytes: ...
