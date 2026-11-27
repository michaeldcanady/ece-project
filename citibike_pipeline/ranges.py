"""Offset ranges: what a batch reads, per topic and partition, and the arithmetic over them."""
from __future__ import annotations

from typing import TYPE_CHECKING, List, Mapping, MutableMapping, Optional, Tuple

if TYPE_CHECKING:
    pass



Ranges = Mapping[str, Mapping[int, Tuple[int, int]]]


MutableRanges = MutableMapping[str, MutableMapping[int, Tuple[int, int]]]


def ranges_to_json(ranges: Ranges) -> Mapping[str, Mapping[str, List[int]]]:
    return {t: {str(p): [s, e] for p, (s, e) in parts.items()} for t, parts in ranges.items()}


def ranges_from_json(obj: object) -> Ranges:
    """Parse `ranges_to_json`'s output. Raises ValueError when `obj` is not that shape."""
    if not isinstance(obj, Mapping):
        raise ValueError("ranges must be an object, got %r" % (obj,))
    ranges: MutableRanges = {}
    for topic, parts in obj.items():
        if not isinstance(parts, Mapping):
            raise ValueError("ranges of %r must be an object, got %r" % (topic, parts))
        ranges[str(topic)] = {}
        for pid, bounds in parts.items():
            if not (isinstance(bounds, (list, tuple)) and len(bounds) == 2
                    and isinstance(bounds[0], int) and isinstance(bounds[1], int)):
                raise ValueError("range of %s/%s must be [start, end], got %r" % (topic, pid, bounds))
            ranges[str(topic)][int(str(pid))] = (bounds[0], bounds[1])
    return ranges


def total_messages(ranges: Ranges) -> int:
    return sum(e - s for parts in ranges.values() for (s, e) in parts.values())


def cap_ranges(positions: Ranges, max_per_partition: Optional[int] = None) -> Ranges:
    """The non-empty (start, end) ranges of `positions`, at most `max_per_partition` long."""
    ranges: MutableRanges = {}
    for topic, parts in positions.items():
        for pid, (start, high) in parts.items():
            end = high if max_per_partition is None else min(high, start + max_per_partition)
            if end > start:
                ranges.setdefault(topic, {})[pid] = (start, end)
    return ranges


def fits_within(ranges: Ranges, positions: Ranges) -> bool:
    """True if every range starts where `positions` starts and ends at or below its high watermark."""
    for topic, parts in ranges.items():
        for pid, (start, end) in parts.items():
            pos = positions.get(topic, {}).get(pid)
            if pos is None or pos[0] != start or end > pos[1]:
                return False
    return True
