"""PollRun: what one poll knows from its start."""
from __future__ import annotations

from dataclasses import dataclass

from .config import Config
from .fetch_result import FetchResult
from .job_state import JobState
from .kafka_sink import KafkaSink
from .poll_source import PollSource


@dataclass(frozen=True)
class PollRun:
    """What one poll knows from its start: who polls, from which state, and the fetch."""
    source: PollSource
    state: JobState
    cfg: Config
    sink: KafkaSink
    poll_id: str
    res: FetchResult
