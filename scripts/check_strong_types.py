"""Fail on weak types in annotations: `Any`, `typing.Dict` and the builtin `dict`.

The rule and what to use instead are in CODING_STANDARDS.md ("Strong types"). Files written before the rule
are listed in BASELINE with the number of weak annotations they had; a file may only go down from there.
When you remove some, lower its count here (the check tells you to), so the gain cannot be spent again.

Usage: python scripts/check_strong_types.py   (run by `mise run lint`)
"""
from __future__ import annotations

import ast
import os
import sys
from typing import Iterator, List, Optional, Tuple

ROOTS = ("citibike_contract", "citibike_pipeline", "citibike_producer", "dags", "scripts", "tests")
WEAK = frozenset({"Any", "Dict", "dict"})

# path -> weak annotations allowed (the count when the rule was added). Only ever lower these.
BASELINE = {
    "citibike_contract/__init__.py": 2,
    "citibike_contract/envelope.py": 5,
    "citibike_pipeline/kafka_io.py": 5,
    "citibike_pipeline/lake.py": 6,
    "citibike_pipeline/metrics.py": 4,
    "citibike_pipeline/quality.py": 19,
    "citibike_pipeline/station_info.py": 5,
    "citibike_pipeline/storage.py": 3,
    "citibike_pipeline/validation.py": 4,
    "citibike_producer/broker.py": 4,
    "citibike_producer/create_topics.py": 4,
    "citibike_producer/jobs.py": 4,
    "citibike_producer/sink.py": 5,
    "citibike_producer/sources.py": 12,
    "citibike_producer/synthetic.py": 7,
    "citibike_producer/task.py": 2,
    "citibike_producer/tasks.py": 6,
    "dags/_common.py": 2,
}


def _weak_names(annotation: Optional[ast.expr]) -> Iterator[ast.AST]:
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


def main() -> int:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    failed = False
    for path in sorted(python_files(root)):
        found = violations(os.path.join(root, path))
        allowed = BASELINE.get(path, 0)
        if len(found) > allowed:
            failed = True
            print("%s: %d weak annotation(s), %d allowed (see CODING_STANDARDS.md, Strong types)"
                  % (path, len(found), allowed))
            for line, name in found:
                print("  %s:%d: %s" % (path, line, name))
        elif len(found) < allowed:
            failed = True
            print("%s: %d weak annotation(s), baseline says %d: lower BASELINE in %s to %d"
                  % (path, len(found), allowed, os.path.relpath(__file__, root), len(found)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
