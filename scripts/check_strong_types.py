"""Fail on weak types in annotations: `Any`, `typing.Dict`, the builtin `dict`, and `object` used as a stand-in.

`object` is `Any` with the name changed: `Mapping[str, object]`, `List[object]` or a return of `object` still leave
every reader guessing. It is allowed in one place only: a bare parameter annotation (`def from_json(obj: object)`),
the entry of a parser that is about to turn unchecked input into its class. Anywhere else, name the real type or
write a dataclass. Files that had `object` before this rule are listed in OBJECT_BASELINE with their count; a file
may only go down from there, and the check tells you to lower the count when it does.

The rule and what to use instead are in CODING_STANDARDS.md ("Strong types").

Usage: python scripts/check_strong_types.py   (run by `mise run lint`)
"""
from __future__ import annotations

import ast
import os
import sys
from typing import Iterator, List, Mapping, MutableMapping, Optional, Tuple, Union

ROOTS = ("citibike_contract", "citibike_pipeline", "citibike_producer", "dags", "scripts", "tests")
WEAK = frozenset({"Any", "Dict", "dict"})

# path -> `object` uses allowed (the count when the rule was added). Only ever lower these.
OBJECT_BASELINE: Mapping[str, int] = {
    "citibike_contract/__init__.py": 1,
    "citibike_contract/envelope.py": 1,
    "citibike_contract/poll_log.py": 1,
    "citibike_pipeline/batch_log.py": 2,
    "citibike_pipeline/counts.py": 1,
    "citibike_pipeline/daily_quality_report.py": 2,
    "citibike_pipeline/dim_station_build.py": 1,
    "citibike_pipeline/lake_table.py": 2,
    "citibike_pipeline/poll_section.py": 1,
    "citibike_pipeline/rows.py": 1,
    "citibike_pipeline/s3_client.py": 3,
    "citibike_pipeline/schemas.py": 1,
    "citibike_pipeline/serving.py": 1,
    "citibike_pipeline/status.py": 3,
    "citibike_pipeline/storage_ops.py": 1,
    "citibike_producer/jsonvalues.py": 1,
    "citibike_producer/producer_like.py": 3,
    "citibike_producer/station_feed.py": 1,
    "citibike_producer/synthetic.py": 5,
    "citibike_producer/task_instance_like.py": 1,
    "dags/_common.py": 1,
    "tests/e2e_live/gold_checks.py": 3,
}


def _weak_names(annotation: Optional[ast.expr]) -> Iterator[Union[ast.Name, ast.Attribute]]:
    if annotation is None:
        return
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):   # quoted annotation
        try:
            annotation = ast.parse(annotation.value, mode="eval").body
        except SyntaxError:
            return
    for node in ast.walk(annotation):
        if isinstance(node, ast.Name) and node.id in WEAK:
            yield node
        elif isinstance(node, ast.Attribute) and node.attr in WEAK:
            yield node


def _annotations(tree: ast.AST) -> Iterator[Optional[ast.expr]]:
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign):
            yield node.annotation
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node.returns
            a = node.args
            for arg in a.posonlyargs + a.args + a.kwonlyargs + [a.vararg, a.kwarg]:
                if arg is not None:
                    yield arg.annotation


def _is_object(node: Optional[ast.AST]) -> bool:
    return isinstance(node, ast.Name) and node.id == "object"


def _object_uses(tree: ast.AST) -> List[int]:
    """Line of each `object` in an annotation or type alias, except a bare parameter annotation."""
    lines: List[int] = []

    def scan(annotation: Optional[ast.expr], line: Optional[int] = None) -> None:
        if annotation is None:
            return
        for n in ast.walk(annotation):
            if not isinstance(n, (ast.Name, ast.Constant)):
                continue
            at = line if line is not None else n.lineno   # a quoted annotation's own lines are relative to its string
            if _is_object(n):
                lines.append(at)
            elif isinstance(n, ast.Constant) and isinstance(n.value, str):     # `"object"`, `List["object"]`
                try:
                    inner = ast.parse(n.value, mode="eval").body
                except SyntaxError:
                    continue
                scan(inner, at)

    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign):
            scan(node.annotation)
            if isinstance(node.value, ast.Subscript):          # `X: TypeAlias = Mapping[str, object]`
                scan(node.value)
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Subscript):   # `X = Mapping[str, object]`
            scan(node.value)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scan(node.returns)
            a = node.args
            for arg in a.posonlyargs + a.args + a.kwonlyargs + [a.vararg, a.kwarg]:
                if arg is not None and not _is_object(arg.annotation):
                    scan(arg.annotation)
    return sorted(lines)


def violations(path: str) -> List[Tuple[int, str]]:
    with open(path, encoding="utf-8") as fh:
        tree = ast.parse(fh.read(), filename=path)
    found = []
    for annotation in _annotations(tree):
        for node in _weak_names(annotation):
            name = node.id if isinstance(node, ast.Name) else node.attr
            found.append((getattr(annotation, "lineno", 0), name))
    return sorted(found)


def python_files(root: str) -> Iterator[str]:
    for top in ROOTS:
        for dirpath, dirnames, filenames in os.walk(os.path.join(root, top)):
            dirnames[:] = [d for d in dirnames if not d.startswith(".") and d != "__pycache__"]
            for name in filenames:
                if name.endswith(".py"):
                    yield os.path.relpath(os.path.join(dirpath, name), root).replace(os.sep, "/")


def object_uses(path: str) -> List[int]:
    with open(path, encoding="utf-8") as fh:
        return _object_uses(ast.parse(fh.read(), filename=path))


def object_failures(uses: Mapping[str, List[int]], baseline: Mapping[str, int]) -> List[str]:
    failures = []
    for path in sorted(set(uses) | set(baseline)):
        lines = uses.get(path, [])
        found, allowed = len(lines), baseline.get(path, 0)
        if found > allowed:
            failures.append("%s: %d use(s) of `object` as a type, %d allowed (lines %s); name the real type or write "
                            "a dataclass" % (path, found, allowed, ", ".join(str(n) for n in lines)))
        elif found < allowed:
            failures.append("%s: %d use(s) of `object` left, %d allowed; lower OBJECT_BASELINE to %d"
                            % (path, found, allowed, found))
    return failures


def main() -> int:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    failed = False
    uses_by_path: MutableMapping[str, List[int]] = {}
    for path in sorted(python_files(root)):
        uses = object_uses(os.path.join(root, path))
        if uses:
            uses_by_path[path] = uses
    for failure in object_failures(uses_by_path, OBJECT_BASELINE):
        failed = True
        print(failure)
    for path in sorted(python_files(root)):
        found = violations(os.path.join(root, path))
        if found:
            failed = True
            print("%s: %d weak annotation(s) (see CODING_STANDARDS.md, Strong types)" % (path, len(found)))
            for line, name in found:
                print("  %s:%d: %s" % (path, line, name))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
