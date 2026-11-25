"""The layer check, through its entry point: a source root, the layer map and the baseline in; an exit code and the
printed report out. Also run once on the real repository."""
import importlib.util
import os
from typing import FrozenSet, Mapping, Tuple

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
_spec = importlib.util.spec_from_file_location("check_layers", os.path.join(ROOT, "scripts", "check_layers.py"))
check_layers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_layers)

CHAINS = {"contract": ["contract"], "app": ["core", "infra", "entry"]}
LAYER_MAP = {
    "pkg": "app:core",
    "pkg.core_a": "app:core",
    "pkg.core_b": "app:core",
    "pkg.infra": "app:infra",
    "pkg.entry": "app:entry",
}


def _tree(tmp_path, files: Mapping[str, str]) -> str:
    for rel, text in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return str(tmp_path)


def _run(tmp_path, files, layer_map=LAYER_MAP, baseline: FrozenSet[Tuple[str, str]] = frozenset(), capsys=None):
    root = _tree(tmp_path, {"pkg/__init__.py": "", "pkg/core_a.py": "", "pkg/core_b.py": "",
                            "pkg/infra.py": "", "pkg/entry.py": "", **files})
    code = check_layers.main(root, layer_map, baseline, CHAINS, ("pkg",))
    return code, capsys.readouterr().out


def test_import_of_a_lower_layer_passes(tmp_path, capsys):
    code, out = _run(tmp_path, {"pkg/entry.py": "from pkg import infra\nimport pkg.core_a\n"}, capsys=capsys)
    assert (code, out) == (0, "")


def test_import_within_a_layer_passes(tmp_path, capsys):
    code, out = _run(tmp_path, {"pkg/core_a.py": "from pkg.core_b import x\n"}, capsys=capsys)
    assert (code, out) == (0, "")


def test_import_of_a_higher_layer_fails_naming_both_modules_and_layers(tmp_path, capsys):
    code, out = _run(tmp_path, {"pkg/core_a.py": "from pkg.infra import x\n"}, capsys=capsys)
    assert code == 1
    assert "pkg.core_a (app: core) imports pkg.infra (app: infra)" in out


def test_violation_in_the_baseline_passes(tmp_path, capsys):
    code, out = _run(tmp_path, {"pkg/core_a.py": "from pkg.infra import x\n"},
                     baseline=frozenset({("pkg.core_a", "pkg.infra")}), capsys=capsys)
    assert (code, out) == (0, "")


def test_baseline_entry_that_no_longer_occurs_fails_and_says_to_remove_it(tmp_path, capsys):
    code, out = _run(tmp_path, {}, baseline=frozenset({("pkg.core_a", "pkg.infra")}), capsys=capsys)
    assert code == 1
    assert "remove" in out and "pkg.core_a" in out and "pkg.infra" in out


def test_module_in_no_layer_fails(tmp_path, capsys):
    code, out = _run(tmp_path, {"pkg/stray.py": ""}, capsys=capsys)
    assert code == 1
    assert "pkg.stray is in no layer" in out


@pytest.mark.parametrize("line", ["from . import infra", "from .infra import x", "from pkg.infra import x",
                                  "import pkg.infra"])
def test_relative_and_absolute_imports_resolve_to_the_same_module(tmp_path, capsys, line):
    code, out = _run(tmp_path, {"pkg/core_a.py": line + "\n"}, capsys=capsys)
    assert code == 1
    assert "pkg.core_a (app: core) imports pkg.infra (app: infra)" in out


def test_real_repository_has_no_violation_outside_the_baseline(capsys):
    code = check_layers.main(ROOT)
    assert (code, capsys.readouterr().out) == (0, "")
