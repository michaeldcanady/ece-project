"""The test ratchet through its interface: sources in, counts and findings out. Also run once on the real `tests/`,
so the check cannot rot unnoticed."""
import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, os.path.join(ROOT, "scripts"))  # the script imports its sibling `test_counts`
_spec = importlib.util.spec_from_file_location(
    "check_test_ratchet", os.path.join(ROOT, "scripts", "check_test_ratchet.py"))
check_test_ratchet = importlib.util.module_from_spec(_spec)
sys.modules["check_test_ratchet"] = check_test_ratchet
_spec.loader.exec_module(check_test_ratchet)
counts = check_test_ratchet.TestCounts

TWO_TESTS = """\
import pytest

def helper():
    assert True

def test_a():
    assert 1
    assert 2

class TestGroup:
    def test_b(self):
        with pytest.raises(ValueError):
            pass

    def not_a_test(self):
        assert True
"""
NO_ASSERTS = TWO_TESTS.replace("assert 1", "pass").replace("assert 2", "pass")
ONE_TEST = "def test_a():\n    assert 1\n    assert 2\n"


def test_count_source_counts_tests_in_classes_and_assertions():
    # helper() and not_a_test() hold an assert each, so they count as assertions but not as tests
    assert check_test_ratchet.count_source(TWO_TESTS) == counts(tests=2, assertions=5)


def test_count_source_ignores_non_test_names():
    assert check_test_ratchet.count_source("def helper():\n    pass\n") == counts(0, 0)


def test_count_tree_ignores_files_outside_tests():
    tree = {"tests/test_x.py": ONE_TEST, "tests/helper.py": ONE_TEST, "scripts/test_y.py": ONE_TEST,
            "tests/sub/test_z.py": ONE_TEST}
    assert check_test_ratchet.count_tree(tree) == counts(1, 2)


def test_compare_passes_on_equal_and_on_growth():
    base = counts(2, 4)
    assert check_test_ratchet.compare(base, base) == []
    assert check_test_ratchet.compare(base, counts(3, 9)) == []


def test_compare_reports_a_removed_test():
    base = check_test_ratchet.count_tree({"tests/test_x.py": TWO_TESTS})
    head = check_test_ratchet.count_tree({"tests/test_x.py": ONE_TEST})
    assert check_test_ratchet.compare(base, head) == [
        "tests dropped from 2 to 1", "assertions dropped from 5 to 2"]


def test_compare_reports_assertions_replaced_by_pass():
    base = check_test_ratchet.count_tree({"tests/test_x.py": TWO_TESTS})
    head = check_test_ratchet.count_tree({"tests/test_x.py": NO_ASSERTS})
    assert head.tests == base.tests
    assert check_test_ratchet.compare(base, head) == ["assertions dropped from 5 to 3"]


def test_compare_reports_fewer_tests_alone():
    assert check_test_ratchet.compare(counts(2, 4), counts(1, 4)) == ["tests dropped from 2 to 1"]


def test_main_without_base_skips(capsys):
    assert check_test_ratchet.main([]) == 0
    assert "skipped" in capsys.readouterr().out


def test_the_real_tests_directory_has_tests():
    files = {f"tests/{p.name}": p.read_text() for p in (ROOT / "tests").glob("test_*.py")}
    found = check_test_ratchet.count_tree(files)
    assert found.tests > 0
    assert found.assertions > 0
