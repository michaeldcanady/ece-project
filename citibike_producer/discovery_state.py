"""What GBFS discovery found, kept between task runs so the next run can skip re-fetching gbfs.json."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional

from citibike_contract import JsonObject

from .jsonvalues import optional_str


@dataclass(frozen=True)
class DiscoveryState:
    urls: Mapping[str, str]
    version: Optional[str]
    loaded_at: float

    def to_json(self) -> JsonObject:
        return {"urls": dict(self.urls), "version": self.version, "loaded_at": self.loaded_at}

    @staticmethod
    def from_json(obj: object) -> Optional[DiscoveryState]:
        """The state, or None when `obj` holds no feed URLs (nothing worth restoring)."""
        if not isinstance(obj, Mapping) or not isinstance(obj.get("urls"), Mapping):
            return None
        urls = {str(k): str(v) for k, v in obj["urls"].items()}
        loaded_at = obj.get("loaded_at")
        if not urls:
            return None
        return DiscoveryState(urls=urls, version=optional_str(obj.get("version")),
                              loaded_at=float(loaded_at) if isinstance(loaded_at, (int, float)) else 0.0)
