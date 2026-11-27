"""The error for an envelope that has no Kafka key to be sent under."""
from __future__ import annotations


class MissingKafkaKey(ValueError):
    """The envelope's payload lacks the id its Kafka key is made from (station_id or location_id).
    Such an envelope cannot be sent: the producer skips it and logs why."""
