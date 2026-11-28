"""Fail when the tests of a change are fewer, or assert less, than the tests of the base branch.

Counting is static through `ast`: no test is imported or run. A test is a function named `test_*` in a
`tests/test_*.py` file, at module level or in a class. An assertion is an `assert` statement or a
`with pytest.raises(...)` block. A parametrized test counts once, however many cases it has; this is a known limit.

Without `--base` the check is skipped. A pull request that drops tests on purpose needs a reviewer's sign-off in CI.

Usage: python scripts/check_test_ratchet.py --base <git ref>   (run by `mise run check:ratchet`)
"""
from __future__ import annotations

import argparse
import ast
import fnmatch
import subprocess
import sys
from pathlib import Path
from typing import List, Mapping, Sequence

from test_counts import TestCounts

TESTS_DIR = "tests"
TEST_FILE = "test_*.py"


def _is_raises(item: ast.withitem) -> bool:
    call = item.context_expr
    return (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
            and call.func.attr == "raises" and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "pytest")


def count_source(source: str) -> TestCounts:
    """Tests and assertions in one file's source."""
    tests = assertions = 0
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"):
            tests += 1
        elif isinstance(node, ast.Assert):
            assertions += 1
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            assertions += sum(1 for item in node.items if _is_raises(item))
    return TestCounts(tests, assertions)


def _is_test_file(path: str) -> bool:
    parent, _, name = path.rpartition("/")
    return parent == TESTS_DIR and fnmatch.fnmatch(name, TEST_FILE)


def count_tree(files: Mapping[str, str]) -> TestCounts:
    """Tests and assertions in a tree of path -> source; only `tests/test_*.py` counts."""
    counts = [count_source(src) for path, src in files.items() if _is_test_file(path)]
    return TestCounts(sum(c.tests for c in counts), sum(c.assertions for c in counts))


def compare(base: TestCounts, head: TestCounts) -> List[str]:
    """Findings where `head` has fewer tests or assertions than `base`; empty means pass."""
    findings: List[str] = []
    if head.tests < base.tests:
        findings.append(f"tests dropped from {base.tests} to {head.tests}")
    if head.assertions < base.assertions:
        findings.append(f"assertions dropped from {base.assertions} to {head.assertions}")
    return findings


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def _read_head(root: Path) -> Mapping[str, str]:
    return {f"{TESTS_DIR}/{p.name}": p.read_text()
            for p in sorted((root / TESTS_DIR).glob(TEST_FILE))}


def _read_base(ref: str) -> Mapping[str, str]:
    paths = _git("ls-tree", "--name-only", f"{ref}:{TESTS_DIR}/").split()
    return {f"{TESTS_DIR}/{name}": _git("show", f"{ref}:{TESTS_DIR}/{name}")
            for name in paths if fnmatch.fnmatch(name, TEST_FILE)}


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="Fail when tests or assertions drop below the base ref.")
    parser.add_argument("--base", help="git ref of the base branch, e.g. origin/main")
    args = parser.parse_args(argv)
    if not args.base:
        print("no base ref, skipped")
        return 0
    base = count_tree(_read_base(args.base))
    head = count_tree(_read_head(Path(__file__).resolve().parent.parent))
    findings = compare(base, head)
    for finding in findings:
        print(f"test ratchet: {finding}")
    if not findings:
        print(f"test ratchet ok: {head.tests} tests, {head.assertions} assertions "
              f"(base {base.tests}, {base.assertions})")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
