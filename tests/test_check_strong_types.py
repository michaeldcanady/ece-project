from __future__ import annotations

import ast
import os
import sys
from typing import List

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))

import check_strong_types as cst  # noqa: E402


def _uses(source: str) -> List[int]:
    return cst._object_uses(ast.parse(source))


def test_bare_object_parameter_is_the_allowed_parser_entry() -> None:
    assert _uses("def from_json(obj: object) -> int: ...") == []


def test_object_inside_a_generic_is_rejected() -> None:
    assert _uses("def f(x: Mapping[str, object]) -> None: ...") == [1]
    assert _uses("def f(x: Sequence[object]) -> None: ...") == [1]


def test_object_return_is_rejected() -> None:
    assert _uses("def f() -> object: ...") == [1]


def test_object_in_a_variable_annotation_is_rejected() -> None:
    assert _uses("x: object = 1") == [1]


def test_object_in_a_type_alias_is_rejected() -> None:
    assert _uses("JsonObject = Mapping[str, object]") == [1]


def test_quoted_object_annotation_is_rejected() -> None:
    assert _uses("def f() -> 'Mapping[str, object]': ...") == [1]


def test_object_in_a_nested_string_is_rejected() -> None:
    assert _uses("def f(x: List['object']) -> None: ...") == [1]
    assert _uses("def f(x: Optional['object']) -> None: ...") == [1]
    assert _uses("def f(x: List['Mapping[str, object]']) -> None: ...") == [1]


def test_quoted_use_reports_the_file_line() -> None:
    assert _uses("\n\ndef f(x: List['object']) -> None: ...") == [3]


def test_baseline_may_only_shrink() -> None:
    assert cst.object_failures({"a.py": [1, 2]}, {"a.py": 2}) == []
    assert "1 allowed" in cst.object_failures({"a.py": [1, 2]}, {"a.py": 1})[0]
    assert "lines 4, 9" in cst.object_failures({"a.py": [4, 9]}, {"a.py": 1})[0]
    assert "lower OBJECT_BASELINE" in cst.object_failures({"a.py": [1]}, {"a.py": 2})[0]
    assert cst.object_failures({"new.py": [7]}, {})


def test_string_subscripts_that_are_not_types_are_not_flagged() -> None:
    assert _uses("kind = payload['object']") == []
    assert _uses("Mode = Literal['object']") == []
    assert _uses("def f(x: Literal['object']) -> None: ...") == []


def _weak(source: str) -> List[str]:
    tree = ast.parse(source)
    return [name for a in cst._annotations(tree) for name, _ in cst._weak_names(a)]


def test_nested_quoted_weak_names_are_flagged() -> None:
    assert _weak("def f(x: List['dict']) -> None: ...") == ["dict"]
    assert _weak("x: Optional['Any'] = None") == ["Any"]


def test_annotated_metadata_is_not_a_type() -> None:
    assert _uses("def f(x: Annotated[int, 'object']) -> None: ...") == []
    assert _uses("def f(x: Annotated[Sequence[object], 'meta']) -> None: ...") == [1]
