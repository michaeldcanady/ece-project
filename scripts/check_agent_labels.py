"""Fail when the agent labels or the spec heading disagree across files.

Two kinds of string have to agree, and nothing else checks that they do:

- **Labels.** Every `agent:*`, `strength:*` or `door:*` label a workflow or a `.sandcastle/` TypeScript file uses
  must be created by the `"name|description|color"` list in `.sandcastle/setup.sh`: `gh issue edit --add-label`
  fails on a label that does not exist.
- **The spec heading.** The heading the implement guard (`implement.yml`) and the spec schema (`spec-plan.ts`) require
  must be one of the headings `spec-format.md` defines, or the guard blocks every issue and the schema rejects every
  spec.

Usage: python scripts/check_agent_labels.py   (run by `mise run lint`)
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import FrozenSet, Iterator, List, Pattern, Sequence, Tuple

SETUP_SH = ".sandcastle/setup.sh"
SPEC_FORMAT = ".sandcastle/shared/spec-format.md"
# file -> the pattern that finds the heading it requires
HEADING_USERS: Tuple[Tuple[str, Pattern[str]], ...] = (
    (".sandcastle/spec/spec-plan.ts", re.compile(r'\.includes\("(##[^"]+)"\)')),
    (".github/workflows/implement.yml", re.compile(r'contains\("(##[^"]+)"\)')),
)

LABEL = re.compile(r"\b(?:agent:[a-z-]+|strength:[a-z]+|door:[a-z-]+)")
CREATED = re.compile(r'"((?:agent|strength|door):[a-z-]+)\|[^|"]*\|[0-9a-fA-F]{6}"')
FORMAT_HEADING = re.compile(r"`(##[^`]+)`")


@dataclass(frozen=True)
class Finding:
    """One disagreement: the file it is in, and what is wrong."""

    path: str
    message: str


def _users(root: Path) -> Iterator[Path]:
    """The workflows and the `.sandcastle/` TypeScript files, outside `node_modules`."""
    workflows = root / ".github" / "workflows"
    paths = sorted(workflows.glob("*.yml")) + sorted(workflows.glob("*.yaml"))
    paths += sorted((root / ".sandcastle").rglob("*.ts"))
    for path in paths:
        if "node_modules" not in path.relative_to(root).parts:
            yield path


def _labels_in(text: str) -> FrozenSet[str]:
    return frozenset(LABEL.findall(text))


def used_labels(root: Path) -> FrozenSet[str]:
    """Every agent or strength label a workflow or a `.sandcastle/` TypeScript file names."""
    found: FrozenSet[str] = frozenset()
    for path in _users(root):
        found |= _labels_in(path.read_text(encoding="utf-8"))
    return found


def created_labels(setup_sh: str) -> FrozenSet[str]:
    """The labels `setup.sh` creates: the names in its `"name|description|color"` lines."""
    return frozenset(CREATED.findall(setup_sh))


def _read(root: Path, rel: str) -> str:
    path = root / rel
    return path.read_text(encoding="utf-8") if path.is_file() else ""


def _label_findings(root: Path) -> List[Finding]:
    created = created_labels(_read(root, SETUP_SH))
    findings: List[Finding] = []
    for path in _users(root):
        rel = path.relative_to(root).as_posix()
        for label in sorted(_labels_in(path.read_text(encoding="utf-8")) - created):
            findings.append(Finding(rel, "label %s is used but %s does not create it" % (label, SETUP_SH)))
    return findings


def _heading_findings(root: Path) -> List[Finding]:
    defined = frozenset(FORMAT_HEADING.findall(_read(root, SPEC_FORMAT)))
    findings: List[Finding] = []
    for rel, pattern in HEADING_USERS:
        match = pattern.search(_read(root, rel))
        if match is None:
            findings.append(Finding(rel, "no required spec heading found (pattern %s)" % pattern.pattern))
        elif match.group(1) not in defined:
            findings.append(Finding(rel, "requires heading %r, which %s does not define" % (
                match.group(1), SPEC_FORMAT)))
    return findings


def check(root: Path) -> Sequence[Finding]:
    """Every label used but not created, and every required spec heading the format does not define."""
    return _label_findings(root) + _heading_findings(root)


def main(root: Path = Path(".")) -> int:
    findings = check(root)
    for finding in findings:
        print("%s: %s" % (finding.path, finding.message))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
