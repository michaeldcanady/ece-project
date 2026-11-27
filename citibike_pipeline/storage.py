"""Object storage abstraction. Same keys locally and on S3; failures are loud (no silent fallbacks)."""
from __future__ import annotations

from typing import List, Protocol


class Storage(Protocol):
    def put(self, key: str, data: bytes) -> None: ...

    def get(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...

    def list(self, prefix: str) -> List[str]: ...

    def delete(self, key: str) -> None: ...








# ---- helpers -------------------------------------------------------------------------------







