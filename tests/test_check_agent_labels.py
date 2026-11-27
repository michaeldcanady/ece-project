"""The agent label and spec heading check, through `check(root)`: a repository tree in, the findings out. Also run
once on the real repository, so the check cannot rot unnoticed."""
import importlib.util
import os
import sys
from pathlib import Path
from typing import Mapping

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "check_agent_labels", os.path.join(ROOT, "scripts", "check_agent_labels.py"))
check_agent_labels = importlib.util.module_from_spec(_spec)
sys.modules["check_agent_labels"] = check_agent_labels  # the dataclass looks its module up here
_spec.loader.exec_module(check_agent_labels)

SETUP_SH = """\
  for spec in \\
    "agent:candidate|Filed by the nightly run|0e8a16" \\
    "agent:implement|Add to start the implement workflow|1d76db" \\
    "strength:strong|Candidate strength: strong|5319e7" \\
    "priority:high|Jump the implement queue|b60205"; do
    IFS='|' read -r name desc color <<<"$spec"
  done
"""
SPEC_FORMAT = "- `## Problem`: the friction.\n- `## Acceptance criteria`: a checklist.\n"
SPEC_PLAN = 'export const specSchema = z.object({ body: z.string().includes("## Acceptance criteria") });\n'
IMPLEMENT = """\
on:
  issues:
    types: [labeled]
jobs:
  run:
    if: github.event.label.name == 'agent:implement'
    steps:
      - run: gh issue view 1 --json body --jq '.body | contains("## Acceptance criteria")'
"""
FILES = {
    ".sandcastle/setup.sh": SETUP_SH,
    ".sandcastle/shared/spec-format.md": SPEC_FORMAT,
    ".sandcastle/spec/spec-plan.ts": SPEC_PLAN,
    ".github/workflows/implement.yml": IMPLEMENT,
    ".sandcastle/improve/apply.ts": 'const LABEL = "agent:candidate"; const S = "strength:strong";\n',
}


def _check(tmp_path: Path, overrides: Mapping[str, str]):
    for rel, text in {**FILES, **overrides}.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return check_agent_labels.check(tmp_path)


def test_all_labels_created_and_headings_agreeing_gives_no_finding(tmp_path: Path):
    assert list(_check(tmp_path, {})) == []


def test_a_used_label_missing_from_setup_sh_gives_one_finding_naming_file_and_label(tmp_path: Path):
    findings = _check(tmp_path, {".github/workflows/spec.yml": "if: contains(labels, 'agent:spec')\n"})
    assert len(findings) == 1
    assert findings[0].path == ".github/workflows/spec.yml"
    assert "agent:spec" in findings[0].message


def test_a_priority_label_must_be_created_like_an_agent_label(tmp_path: Path):
    files = {".github/workflows/implement.yml": IMPLEMENT + "# priority:high agent:recovered\n"}
    findings = _check(tmp_path, files)
    assert [f.message.split()[1] for f in findings] == ["agent:recovered"]


def test_labels_under_node_modules_are_skipped(tmp_path: Path):
    assert list(_check(tmp_path, {".sandcastle/node_modules/x/index.ts": 'const L = "agent:other";\n'})) == []


def test_a_heading_changed_in_spec_format_gives_findings_naming_the_guard_and_the_schema(tmp_path: Path):
    findings = _check(tmp_path, {".sandcastle/shared/spec-format.md": "- `## Done when`: a checklist.\n"})
    assert sorted(f.path for f in findings) == [".github/workflows/implement.yml", ".sandcastle/spec/spec-plan.ts"]
    assert all("## Acceptance criteria" in f.message for f in findings)


def test_a_heading_changed_in_the_schema_gives_a_finding_naming_it(tmp_path: Path):
    findings = _check(tmp_path, {".sandcastle/spec/spec-plan.ts": SPEC_PLAN.replace("criteria", "tests")})
    assert [f.path for f in findings] == [".sandcastle/spec/spec-plan.ts"]


def test_a_guard_with_no_heading_gives_a_finding_naming_it(tmp_path: Path):
    findings = _check(tmp_path, {".github/workflows/implement.yml": "on: push\n"})
    assert [f.path for f in findings] == [".github/workflows/implement.yml"]


def test_the_real_repository_gives_no_finding():
    assert list(check_agent_labels.check(ROOT)) == []
