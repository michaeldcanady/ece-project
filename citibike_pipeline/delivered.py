"""A parsed observation with the Kafka record and the envelope it arrived in."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from citibike_contract import Envelope

from .record import Record

T = TypeVar("T")


@dataclass(frozen=True)
class Delivered(Generic[T]):
    """A parsed observation with the Kafka record and the envelope it arrived in."""
    record: Record
    envelope: Envelope
    value: T
