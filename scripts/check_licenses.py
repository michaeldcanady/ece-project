"""Fail when an installed dependency has no license data or a license that is not on the allow list.

The license comes from `License-Expression` first, then the `License ::` classifiers, then the free-text `License`
field. `LICENSE_NAMES` maps classifier names and common free-text forms to SPDX ids. The allow list is
`scripts/allowed_licenses.txt`.

Usage: python scripts/check_licenses.py   (run by `mise run lint`)
"""
from __future__ import annotations

import re
import sys
from importlib import metadata
from pathlib import Path
from typing import FrozenSet, List, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
from finding import Finding  # noqa: E402
from package import Package  # noqa: E402

PROJECT = "citibike-pipeline"
ALLOWED_FILE = Path(__file__).resolve().parent / "allowed_licenses.txt"

# Lower-case classifier name or free-text form -> SPDX id.
LICENSE_NAMES = {
    "mit license": "MIT",
    "mit": "MIT",
    "bsd license": "BSD-3-Clause",
    "bsd": "BSD-3-Clause",
    "bsd-2-clause": "BSD-2-Clause",
    "bsd-3-clause": "BSD-3-Clause",
    "apache software license": "Apache-2.0",
    "apache license 2.0": "Apache-2.0",
    "apache license, version 2.0": "Apache-2.0",
    "apache 2.0": "Apache-2.0",
    "apache-2.0": "Apache-2.0",
    "python software foundation license": "PSF-2.0",
    "psf-2.0": "PSF-2.0",
    "isc license (iscl)": "ISC",
    "isc license": "ISC",
    "isc": "ISC",
    "mozilla public license 2.0 (mpl 2.0)": "MPL-2.0",
    "mpl-2.0": "MPL-2.0",
}

TOKEN = re.compile(r"\(|\)|[^\s()]+")
HINT = "add a mapping to LICENSE_NAMES in scripts/check_licenses.py or an entry to scripts/allowed_licenses.txt"


def _spdx(name: str) -> str:
    return LICENSE_NAMES.get(name.strip().casefold(), name.strip())


def _any_allowed(tokens: List[str], allowed: FrozenSet[str]) -> bool:
    """`OR` of `AND` groups; parentheses nest. Consumes `tokens` from the front."""
    result = _all_allowed(tokens, allowed)
    while tokens and tokens[0].upper() == "OR":
        tokens.pop(0)
        result = _all_allowed(tokens, allowed) or result
    return result


def _all_allowed(tokens: List[str], allowed: FrozenSet[str]) -> bool:
    result = _atom_allowed(tokens, allowed)
    while tokens and tokens[0].upper() == "AND":
        tokens.pop(0)
        result = _atom_allowed(tokens, allowed) and result
    return result


def _atom_allowed(tokens: List[str], allowed: FrozenSet[str]) -> bool:
    if not tokens:
        return False
    token = tokens.pop(0)
    if token == "(":
        result = _any_allowed(tokens, allowed)
        if tokens and tokens[0] == ")":
            tokens.pop(0)
        return result
    return token in allowed


def expression_allowed(expression: str, allowed: FrozenSet[str]) -> bool:
    """Whether an SPDX expression passes: any `OR` alternative, or every `AND` part, is on the allow list."""
    tokens = TOKEN.findall(expression)
    return _any_allowed(tokens, allowed) and not tokens


def _license_expression(package: Package) -> Optional[str]:
    """The package's license as an SPDX expression, or None when its metadata has none."""
    if package.license_expression and package.license_expression.strip():
        return package.license_expression.strip()
    names = [c.split("::")[-1] for c in package.classifiers if c.startswith("License ::")]
    # "License :: OSI Approved" alone names no license.
    names = [n for n in names if n.strip().casefold() != "osi approved"]
    if names:
        return " OR ".join(_spdx(n) for n in names)  # several classifiers mean a dual license
    if package.license_text and package.license_text.strip():
        text = package.license_text.strip()
        return _spdx(text) if text.casefold() in LICENSE_NAMES else text
    return None


def check(packages: Sequence[Package], allowed: FrozenSet[str]) -> List[Finding]:
    """One finding per package whose license is missing or not allowed, sorted by package name."""
    findings: List[Finding] = []
    for package in sorted(packages, key=lambda p: (p.name.casefold(), p.version)):
        if package.name.casefold() == PROJECT:
            continue
        expression = _license_expression(package)
        label = "%s %s" % (package.name, package.version)
        if expression is None:
            findings.append(Finding(package.name, "%s has no license data: %s" % (label, HINT)))
        elif not expression_allowed(expression, allowed):
            findings.append(Finding(package.name, "%s has license %r, which is not allowed: %s" % (
                label, expression, HINT)))
    return findings


def load_allowed(path: Path = ALLOWED_FILE) -> FrozenSet[str]:
    lines = (line.split("#")[0].strip() for line in path.read_text(encoding="utf-8").splitlines())
    return frozenset(line for line in lines if line)


def installed_packages() -> List[Package]:
    """Every distribution in the current environment, read through `importlib.metadata`."""
    packages: List[Package] = []
    for dist in metadata.distributions():
        meta = dist.metadata
        packages.append(Package(
            name=meta["Name"] or "(unnamed)",
            version=meta["Version"] or "",
            license_expression=meta.get("License-Expression"),
            license_text=meta.get("License"),
            classifiers=tuple(c for c in (meta.get_all("Classifier") or []) if c.startswith("License ::")),
        ))
    return packages


def main() -> int:
    findings = check(installed_packages(), load_allowed())
    for finding in findings:
        print("%s: %s" % (finding.package, finding.message))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
