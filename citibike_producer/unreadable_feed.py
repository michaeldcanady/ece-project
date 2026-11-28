"""The error for a good response whose body a source cannot read."""
from __future__ import annotations


class UnreadableFeed(ValueError):
    """The body lacks what the source needs. Its message becomes the poll log's `error`."""
