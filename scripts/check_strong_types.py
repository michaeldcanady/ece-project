"""Fail on weak types in annotations: `Any`, `typing.Dict` and the builtin `dict`.

The rule and what to use instead are in CODING_STANDARDS.md ("Strong types").

Usage: python scripts/check_strong_types.py   (run by `mise run lint`)
"""
from __future__ import annotations

import ast
import os
import sys
from typing import Iterator, List, Optional, Tuple, Union

ROOTS = ("citibike_contract", "citibike_pipeline", "citibike_producer", "dags", "scripts", "tests")
WEAK = frozenset({"Any", "Dict", "dict"})


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
        if found:
            failed = True
            print("%s: %d weak annotation(s) (see CODING_STANDARDS.md, Strong types)" % (path, len(found)))
            for line, name in found:
                print("  %s:%d: %s" % (path, line, name))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
