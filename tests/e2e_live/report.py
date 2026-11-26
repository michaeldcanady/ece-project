"""Render the check results as Markdown and decide the exit status."""
from __future__ import annotations

from datetime import datetime
from typing import List

from .check_result import CheckResult


def render(results: List[CheckResult], started: datetime, notes: List[str]) -> str:
    lines = ["# Live end-to-end report", "", "Run at %s" % started.strftime("%Y-%m-%d %H:%M:%SZ"), ""]
    lines += ["- " + n for n in notes] + [""]
    lines += ["| | Check | Detail |", "|---|---|---|"]
    for r in results:
        lines.append("| %s | %s | %s |" % ("PASS" if r.passed else "FAIL", r.name, r.detail.replace("|", "/")))
    failed = [r for r in results if not r.passed]
    lines += ["", "**%d of %d checks passed.**" % (len(results) - len(failed), len(results))]
    return "\n".join(lines) + "\n"
