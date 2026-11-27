"""The prompt placeholder check, through `check(root)`: a repository tree in, the findings out. Also run once on the
real repository, so the check cannot rot unnoticed."""
import importlib.util
import os
import sys
from pathlib import Path
from typing import Mapping

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "check_prompt_args", os.path.join(ROOT, "scripts", "check_prompt_args.py"))
check_prompt_args = importlib.util.module_from_spec(_spec)
sys.modules["check_prompt_args"] = check_prompt_args  # the dataclass looks its module up here
_spec.loader.exec_module(check_prompt_args)

SCRIPT = """\
await run({
  promptFile: here("prompt.md"),
  promptArgs: {
    ISSUE_NUMBER,
    RUBRIC: fs.readFileSync(path.join(dir, "a,b.md"), "utf8"),
  },
});
"""
FILES = {
    ".sandcastle/x/run.ts": SCRIPT,
    ".sandcastle/x/prompt.md": "Issue {{ISSUE_NUMBER}}\n{{RUBRIC}}\n",
}


def _check(tmp_path: Path, overrides: Mapping[str, str]):
    for rel, text in {**FILES, **overrides}.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return check_prompt_args.check(tmp_path)


def test_matching_placeholders_and_args_give_no_finding(tmp_path: Path):
    assert list(_check(tmp_path, {})) == []


def test_a_placeholder_with_no_arg_gives_a_finding_naming_it(tmp_path: Path):
    findings = _check(tmp_path, {".sandcastle/x/prompt.md": "{{ISSUE_NUMBER}} {{RUBRIC}} {{MISSING}}\n"})
    assert [f.path for f in findings] == [".sandcastle/x/run.ts"]
    assert "MISSING" in findings[0].message


def test_an_arg_no_placeholder_uses_gives_a_finding_naming_it(tmp_path: Path):
    findings = _check(tmp_path, {".sandcastle/x/prompt.md": "{{ISSUE_NUMBER}}\n"})
    assert len(findings) == 1
    assert "RUBRIC" in findings[0].message


def test_a_missing_prompt_file_gives_a_finding(tmp_path: Path):
    findings = _check(tmp_path, {".sandcastle/x/run.ts": SCRIPT.replace("prompt.md", "gone.md")})
    assert len(findings) == 1
    assert "gone.md" in findings[0].message


def test_a_placeholder_written_with_spaces_or_lowercase_is_still_found(tmp_path: Path):
    findings = _check(tmp_path, {".sandcastle/x/prompt.md": "{{ISSUE_NUMBER}} {{RUBRIC}} {{ spaced }} {{lower}}\n"})
    assert sorted(f.message.split("{{")[1].split("}}")[0] for f in findings) == ["lower", "spaced"]


def test_sandcastle_built_in_placeholders_need_no_arg(tmp_path: Path):
    prompt = "{{ISSUE_NUMBER}} {{RUBRIC}} {{SOURCE_BRANCH}} {{TARGET_BRANCH}}\n"
    findings = _check(tmp_path, {".sandcastle/x/prompt.md": prompt})
    assert list(findings) == []


def test_a_call_without_prompt_args_is_not_paired_with_the_next_calls(tmp_path: Path):
    script = SCRIPT + 'await run({ promptFile: here("other.md") });\n'
    findings = _check(tmp_path, {".sandcastle/x/run.ts": script, ".sandcastle/x/other.md": "{{ISSUE_NUMBER}}\n"})
    assert len(findings) == 1
    assert "other.md" in findings[0].message and "ISSUE_NUMBER" in findings[0].message


def test_a_call_passing_prompt_args_as_a_variable_is_skipped(tmp_path: Path):
    script = 'await run({ promptFile: here("other.md"), promptArgs: args });\n' + SCRIPT
    assert list(_check(tmp_path, {".sandcastle/x/run.ts": script, ".sandcastle/x/other.md": "{{WHATEVER}}\n"})) == []


def test_the_real_repository_gives_no_finding():
    assert list(check_prompt_args.check(ROOT)) == []
