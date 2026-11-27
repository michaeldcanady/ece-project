"""The paginator of an S3 client's object listing."""
from __future__ import annotations

from typing import Iterable, Mapping, Protocol, Sequence


class S3Paginator(Protocol):
    def paginate(self, *, Bucket: str, Prefix: str) -> Iterable[Mapping[str, Sequence[Mapping[str, str]]]]: ...
