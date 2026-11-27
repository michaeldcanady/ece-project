"""PollDelivery: the delivery tally of one poll."""
from __future__ import annotations

from dataclasses import dataclass

from .delivery_batch import DeliveryBatch


@dataclass(frozen=True)
class PollDelivery:
    batch: DeliveryBatch
    skipped: int
