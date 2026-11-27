"""Storage on the local file system."""
from __future__ import annotations

import os
import tempfile
from typing import List


class LocalStorage:
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
