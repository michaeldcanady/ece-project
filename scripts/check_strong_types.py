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
OBJECT_BASELINE: Mapping[str, int] = {}


def _is_literal(node: ast.AST) -> bool:
    if not isinstance(node, ast.Subscript):
        return False
    base = node.value
    return (isinstance(base, ast.Name) and base.id == "Literal") or (
        isinstance(base, ast.Attribute) and base.attr == "Literal")


TypeName = Union[ast.Name, ast.Attribute]


def _type_names(annotation: Optional[ast.expr], line: Optional[int] = None) -> Iterator[Tuple[TypeName, int]]:
    """Each Name and Attribute in an annotation with its line, including those inside quoted parts such as
    `List["dict"]`. The strings inside `Literal[...]` are values, not types, and are skipped."""
    if annotation is None:
        return
    pending: List[ast.expr] = [annotation]
    while pending:
        n = pending.pop()
        if _is_literal(n):
            continue
        at = line if line is not None else getattr(n, "lineno", 0)   # a quoted annotation's lines are relative
        if isinstance(n, (ast.Name, ast.Attribute)):
            yield n, at
        elif isinstance(n, ast.Constant) and isinstance(n.value, str):
            try:
                inner = ast.parse(n.value, mode="eval").body
            except SyntaxError:
                continue
            yield from _type_names(inner, at)
            continue
        pending.extend(c for c in ast.iter_child_nodes(n) if isinstance(c, ast.expr))


def _weak_names(annotation: Optional[ast.expr]) -> Iterator[Tuple[str, int]]:
    for node, line in _type_names(annotation):
        name = node.id if isinstance(node, ast.Name) else node.attr
        if name in WEAK:
            yield name, line


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


def _is_alias_target(node: ast.Assign) -> bool:
    """`Name = Mapping[str, object]`: a CapWords name, the convention for a type alias."""
    return len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id[:1].isupper()


def _object_uses(tree: ast.AST) -> List[int]:
    """Line of each `object` in an annotation or type alias, except a bare parameter annotation."""
    lines: List[int] = []

    def scan(annotation: Optional[ast.expr]) -> None:
        lines.extend(line for node, line in _type_names(annotation) if _is_object(node))

    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign):
            scan(node.annotation)
            if isinstance(node.value, ast.Subscript) and "TypeAlias" in ast.dump(node.annotation):
                scan(node.value)                                # `X: TypeAlias = Mapping[str, object]`
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Subscript) and _is_alias_target(node):
            scan(node.value)                                    # `X = Mapping[str, object]`
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
        for name, line in _weak_names(annotation):
            found.append((line, name))
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
