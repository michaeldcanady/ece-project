"""Fail when mypy reports more errors in a file than BASELINE allows, or fewer without BASELINE being lowered.

mypy accepts `Any` and `dict`, so this check runs alongside check_strong_types.py, not instead of it. Files that had
errors before mypy was added are listed in BASELINE with their error count; a file may only go down from there. When
you fix some, lower its count here (the check tells you to), so the gain cannot be spent again. The mypy settings are
in pyproject.toml, so `uv run mypy` shows the same errors this check counts.

Usage: python scripts/check_mypy.py   (run by `mise run lint`)
"""
from __future__ import annotations

import re
import sys
from collections import Counter
from typing import List, Mapping

import mypy.api

# path -> mypy errors allowed (the count when the check was added). Only ever lower these.
BASELINE: Mapping[str, int] = {
    "citibike_contract/envelope.py": 1,
    "citibike_pipeline/batch.py": 0,
    "citibike_pipeline/plan.py": 0,
    "citibike_pipeline/stats.py": 1,
    "citibike_pipeline/batch_log.py": 10,
    "citibike_pipeline/counts.py": 1,
    "citibike_pipeline/enrichment.py": 5,
    "citibike_pipeline/kafka_io.py": 0,
    "citibike_pipeline/lake.py": 1,
    "citibike_pipeline/observations.py": 9,
    "citibike_pipeline/quality.py": 0,
    "citibike_pipeline/serving.py": 4,
    "citibike_pipeline/snapshot.py": 0,
    "citibike_pipeline/station_info.py": 0,
    "citibike_pipeline/status.py": 3,
    "citibike_producer/jobs.py": 21,
    "citibike_producer/sources.py": 2,
    "citibike_producer/synthetic.py": 6,
    "scripts/check_strong_types.py": 1,
}

FATAL_STATUS = 2
_ERROR_LINE = re.compile(r"^(?P<path>[^:\s][^:]*):\d+(?::\d+)?: error: ")


def error_counts(report: str) -> Mapping[str, int]:
    counts: Counter[str] = Counter()
    for line in report.splitlines():
        match = _ERROR_LINE.match(line)
        if match:
            counts[match.group("path")] += 1
    return counts


def compare(counts: Mapping[str, int], baseline: Mapping[str, int]) -> List[str]:
    failures = []
    for path in sorted(set(counts) | set(baseline)):
        found = counts.get(path, 0)
        allowed = baseline.get(path, 0)
        if found > allowed:
            failures.append("%s: %d mypy error(s), %d allowed" % (path, found, allowed))
        elif found < allowed:
            failures.append("%s: %d mypy error(s), baseline says %d: lower BASELINE in scripts/check_mypy.py to %d"
                            % (path, found, allowed, found))
    return failures


def main() -> int:
    stdout, stderr, status = mypy.api.run([])
    failures = compare(error_counts(stdout), BASELINE)
    failing = {line.split(":", 1)[0] for line in failures}
    for line in stdout.splitlines():
        match = _ERROR_LINE.match(line)
        if match and match.group("path") in failing:
            print(line)
    for failure in failures:
        print(failure)
    if status == FATAL_STATUS:
        print("mypy failed to run:\n%s%s" % (stdout, stderr))
    return 1 if failures or status == FATAL_STATUS else 0


if __name__ == "__main__":
    sys.exit(main())
