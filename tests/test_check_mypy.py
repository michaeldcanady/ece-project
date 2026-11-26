"""The mypy baseline check: the report parsing and the ratchet are pure, so they are tested without running mypy."""
import importlib.util
import os

ROOT = os.path.join(os.path.dirname(__file__), "..")
_spec = importlib.util.spec_from_file_location("check_mypy", os.path.join(ROOT, "scripts", "check_mypy.py"))
check_mypy = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_mypy)

REPORT = """\
pkg/a.py:3: error: Incompatible types in assignment  [assignment]
pkg/a.py:9:5: error: Item "None" of "Optional[str]" has no attribute "x"  [union-attr]
pkg/a.py:9: note: Right operand is of type "Optional[str]"
pkg/b.py:1: error: Name "y" is not defined  [name-defined]
Found 3 errors in 2 files (checked 5 source files)
"""


def test_error_counts_counts_error_lines_per_path_and_ignores_notes_and_summary():
    assert dict(check_mypy.error_counts(REPORT)) == {"pkg/a.py": 2, "pkg/b.py": 1}


def test_compare_passes_when_counts_equal_baseline_and_unlisted_paths_are_clean():
    assert check_mypy.compare({"pkg/a.py": 2}, {"pkg/a.py": 2}) == []


def test_compare_reports_a_path_over_its_allowance_including_an_unlisted_path():
    assert check_mypy.compare({"pkg/a.py": 3, "pkg/new.py": 1}, {"pkg/a.py": 2}) == [
        "pkg/a.py: 3 mypy error(s), 2 allowed",
        "pkg/new.py: 1 mypy error(s), 0 allowed",
    ]


def test_compare_reports_a_path_under_its_allowance_with_the_number_to_lower_it_to():
    (failure,) = check_mypy.compare({"pkg/a.py": 1}, {"pkg/a.py": 4})
    assert "pkg/a.py" in failure and "lower BASELINE" in failure and failure.endswith("to 1")


def test_compare_reports_a_baseline_path_with_no_errors():
    (failure,) = check_mypy.compare({}, {"pkg/gone.py": 2})
    assert "pkg/gone.py" in failure and failure.endswith("to 0")


def test_main_fails_when_mypy_exits_with_a_fatal_status(monkeypatch, capsys):
    monkeypatch.setattr(check_mypy.mypy.api, "run", lambda args: ("", "config error", 2))
    monkeypatch.setattr(check_mypy, "BASELINE", {})
    assert check_mypy.main() == 1
    assert "config error" in capsys.readouterr().out
