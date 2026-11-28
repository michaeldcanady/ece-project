"""The license check, through `check(packages, allowed)`: plain values in, findings out. Also run once on the real
environment, so the allow list cannot rot unnoticed."""
import importlib.util
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, os.path.join(ROOT, "scripts"))
_spec = importlib.util.spec_from_file_location("check_licenses", os.path.join(ROOT, "scripts", "check_licenses.py"))
check_licenses = importlib.util.module_from_spec(_spec)
sys.modules["check_licenses"] = check_licenses
_spec.loader.exec_module(check_licenses)
from package import Package  # noqa: E402

ALLOWED = frozenset({"MIT", "BSD-3-Clause", "Apache-2.0"})


def _pkg(name: str, expression: Optional[str] = None, text: Optional[str] = None,
         classifiers: Tuple[str, ...] = ()) -> Package:
    return Package(name, "1.0", expression, text, classifiers)


def _names(*packages: Package):
    return [f.package for f in check_licenses.check(packages, ALLOWED)]


def test_an_allowed_license_expression_passes():
    assert _names(_pkg("a", expression="MIT")) == []


def test_a_classifier_only_package_passes():
    assert _names(_pkg("a", classifiers=("License :: OSI Approved :: Apache Software License",))) == []


def test_a_free_text_only_license_maps_to_its_spdx_id_and_passes():
    assert _names(_pkg("a", text="Apache License 2.0")) == []


def test_the_expression_wins_over_the_classifiers():
    classifiers = ("License :: OSI Approved :: MIT License",)
    assert _names(_pkg("a", expression="GPL-3.0-only", classifiers=classifiers)) == ["a"]


def test_two_classifiers_are_a_dual_license():
    classifiers = ("License :: OSI Approved :: BSD License", "License :: OSI Approved :: Apache Software License")
    assert _names(_pkg("a", text="Dual License", classifiers=classifiers)) == []


def test_or_passes_when_any_alternative_is_allowed():
    assert _names(_pkg("a", expression="Apache-2.0 OR GPL-3.0-only")) == []


def test_and_fails_when_any_part_is_not_allowed():
    assert _names(_pkg("a", expression="MIT AND GPL-3.0-only")) == ["a"]


def test_parentheses_group_the_alternatives():
    assert _names(_pkg("a", expression="MIT AND (GPL-3.0-only OR BSD-3-Clause)")) == []


def test_a_gpl_package_fails_and_the_finding_names_it():
    findings = check_licenses.check([_pkg("copyleft", expression="GPL-3.0-only")], ALLOWED)
    assert len(findings) == 1
    assert findings[0].package == "copyleft"
    assert "copyleft" in findings[0].message and "GPL-3.0-only" in findings[0].message


def test_a_package_with_no_license_data_fails():
    findings = check_licenses.check([_pkg("bare")], ALLOWED)
    assert [f.package for f in findings] == ["bare"]
    assert "no license data" in findings[0].message


def test_an_unmapped_free_text_license_fails():
    assert _names(_pkg("a", text="Dual License")) == ["a"]


def test_the_project_itself_is_skipped():
    assert _names(_pkg("citibike-pipeline")) == []


def test_findings_do_not_depend_on_input_order():
    packages = [_pkg("b"), _pkg("c"), _pkg("a")]
    assert _names(*packages) == _names(*reversed(packages)) == ["a", "b", "c"]


def test_the_real_environment_gives_no_finding():
    allowed = check_licenses.load_allowed()
    assert check_licenses.check(check_licenses.installed_packages(), allowed) == []
