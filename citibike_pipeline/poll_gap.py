"""A stretch between two logged polls that ran longer than the gap threshold."""
from __future__ import annotations

from pydantic import BaseModel, Field


class PollGap(BaseModel):
    from_: str = Field(serialization_alias="from")      # `from` on the wire: dump with by_alias=True
    to: str
    seconds: float
