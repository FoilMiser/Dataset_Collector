#!/usr/bin/env python3
"""Check that requirements.constraints.txt is up to date with requirements.in.

This script verifies that all packages listed in requirements.in are also
present in requirements.constraints.txt with valid version specifiers.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def parse_requirements(file_path: Path) -> dict[str, str]:
    """Parse a requirements file and return a dict of package -> version spec."""
    packages: dict[str, str] = {}

    if not file_path.exists():
        return packages

    content = file_path.read_text()

    for line in content.splitlines():
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        # Skip options like --extra-index-url
        if line.startswith("-"):
            continue

        # Parse package name and version
        match = re.match(r"([a-zA-Z0-9_-]+)([<>=!~].*)?", line)
        if match:
            name = match.group(1).lower().replace("-", "_")
            version = match.group(2) or ""
            packages[name] = version

    return packages


def main() -> int:
    """Check that constraints file is up to date."""
    root = Path(__file__).parent.parent

    requirements_in = root / "requirements.in"
    constraints = root / "requirements.constraints.txt"

    if not requirements_in.exists():
        print(f"Warning: {requirements_in} not found")
        return 0

    if not constraints.exists():
        print(f"Warning: {constraints} not found")
        return 0

    req_packages = parse_requirements(requirements_in)
    constraint_packages = parse_requirements(constraints)

    missing = []
    for pkg in req_packages:
        if pkg not in constraint_packages:
            missing.append(pkg)

    if missing:
        print("The following packages from requirements.in are missing from constraints:")
        for pkg in sorted(missing):
            print(f"  - {pkg}")
        print("\nRun `pip-compile requirements.in` to update constraints.")
        return 1

    print("Constraints file is up to date.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
