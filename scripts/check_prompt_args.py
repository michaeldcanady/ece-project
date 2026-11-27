"""Fail when a `.sandcastle/` prompt's `{{PLACEHOLDER}}`s and the `promptArgs` that fill them disagree.

Sandcastle fails the run when a placeholder has no matching value in `promptArgs` and only warns about an unused
arg. Neither fails the typecheck or a test, only the nightly run. Each `promptFile: here("x.md")` in a
`.sandcastle/` TypeScript file is paired with the `promptArgs: { ... }` that follows it in the same call, and with
the file `x.md` next to it.

Usage: python scripts/check_prompt_args.py   (run by `mise run lint`)
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import FrozenSet, Iterator, List, Sequence, Tuple

# Same pattern as `PLACEHOLDER_PATTERN` in Sandcastle: spaces inside the braces and lowercase keys are allowed.
PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
# Sandcastle fills these itself and refuses them in `promptArgs`.
BUILT_IN = frozenset({"SOURCE_BRANCH", "TARGET_BRANCH"})
PROMPT_ARGS_KEY = re.compile(r"promptArgs\b")
PROMPT_FILE = re.compile(r'promptFile:\s*here\("([^"]+)"\)')
PROMPT_ARGS = re.compile(r"promptArgs:\s*\{")
KEY = re.compile(r"\s*([A-Za-z_]\w*)\s*(?=[:,}]|$)")
OPENERS = "([{"
CLOSERS = ")]}"
QUOTES = "\"'`"


@dataclass(frozen=True)
class Finding:
    """One disagreement: the file it is in, and what is wrong."""

    path: str
    message: str


def _object_body(text: str, start: int) -> str:
    """The text between the `{` just before `start` and its matching `}`; quoted strings are skipped."""
    depth, quote, i = 1, "", start
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "\\":
                i += 1
            elif ch == quote:
                quote = ""
        elif ch in QUOTES:
            quote = ch
        elif ch in OPENERS:
            depth += 1
        elif ch in CLOSERS:
            depth -= 1
            if depth == 0:
                return text[start:i]
        i += 1
    return text[start:]


def _top_level_entries(body: str) -> Iterator[str]:
    """The comma-separated entries of an object literal body, ignoring commas inside brackets and strings."""
    depth, quote, begin, i = 0, "", 0, 0
    while i < len(body):
        ch = body[i]
        if quote:
            if ch == "\\":
                i += 1
            elif ch == quote:
                quote = ""
        elif ch in QUOTES:
            quote = ch
        elif ch in OPENERS:
            depth += 1
        elif ch in CLOSERS:
            depth -= 1
        elif ch == "," and depth == 0:
            yield body[begin:i]
            begin = i + 1
        i += 1
    yield body[begin:]


def arg_names(text: str, args_match: re.Match[str]) -> FrozenSet[str]:
    """The keys of the `promptArgs` object literal that `args_match` opens."""
    names: List[str] = []
    for entry in _top_level_entries(_object_body(text, args_match.end())):
        key = KEY.match(entry)
        if key is not None:
            names.append(key.group(1))
    return frozenset(names)


def _pairs(text: str) -> Iterator[Tuple[str, FrozenSet[str]]]:
    """Each `promptFile` in `text` with the keys of the `promptArgs` in the same call.

    The search stops at the next `promptFile`, so a call without `promptArgs` is not paired with a later call's.
    A call whose `promptArgs` is not an object literal (a variable) is skipped: its keys are unknown.
    """
    matches = list(PROMPT_FILE.finditer(text))
    for index, file_match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        window = text[file_match.end():end]
        args_match = PROMPT_ARGS.search(text, file_match.end(), end)
        if args_match is not None:
            yield file_match.group(1), arg_names(text, args_match)
        elif PROMPT_ARGS_KEY.search(window) is None:
            yield file_match.group(1), frozenset()


def check(root: Path) -> Sequence[Finding]:
    """Every placeholder no arg fills, and every arg no placeholder uses."""
    findings: List[Finding] = []
    sandcastle = root / ".sandcastle"
    for ts in sorted(sandcastle.rglob("*.ts")):
        if "node_modules" in ts.relative_to(root).parts:
            continue
        rel = ts.relative_to(root).as_posix()
        for prompt_name, args in _pairs(ts.read_text(encoding="utf-8")):
            prompt = ts.parent / prompt_name
            if not prompt.is_file():
                findings.append(Finding(rel, "promptFile %s does not exist" % prompt_name))
                continue
            used = frozenset(PLACEHOLDER.findall(prompt.read_text(encoding="utf-8"))) - BUILT_IN
            for name in sorted(used - args):
                findings.append(Finding(rel, "%s uses {{%s}} but promptArgs does not fill it" % (prompt_name, name)))
            for name in sorted(args - used):
                findings.append(Finding(rel, "promptArgs %s is not used by %s" % (name, prompt_name)))
    return findings


def main(root: Path = Path(".")) -> int:
    findings = check(root)
    for finding in findings:
        print("%s: %s" % (finding.path, finding.message))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
