from __future__ import annotations

import re
import sys
from pathlib import Path

REQUIREMENTS_IN = Path("requirements.in")
REQUIREMENTS_DEV_IN = Path("requirements-dev.in")
CONSTRAINTS = Path("requirements.constraints.txt")
DEV_CONSTRAINTS = Path("requirements-dev.constraints.txt")


def _strip_inline_comment(line: str) -> str:
    if "#" not in line:
        return line
    return line.split("#", 1)[0].strip()


def _parse_requirement_name(raw: str) -> str | None:
    cleaned = _strip_inline_comment(raw).strip()
    if not cleaned:
        return None
    if cleaned.startswith("-r") or cleaned.startswith("--requirement"):
        return None
    if cleaned.startswith("-"):
        return None
    match = re.split(r"[<>=!~\[\s]", cleaned, maxsplit=1)
    return match[0].strip() or None


def _parse_constraints(path: Path) -> dict[str, str]:
    pins: dict[str, str] = {}
    for line in path.read_text().splitlines():
        cleaned = _strip_inline_comment(line).strip()
        if not cleaned or cleaned.startswith("-r") or cleaned.startswith("--requirement"):
            continue
        match = re.match(r"^([A-Za-z0-9_.-]+)\s*==\s*([^\s;]+)", cleaned)
        if match:
            name, version = match.groups()
            pins[name.lower()] = version
    return pins


def _parse_requirements(path: Path) -> set[str]:
    names: set[str] = set()
    for line in path.read_text().splitlines():
        name = _parse_requirement_name(line)
        if name:
            names.add(name.lower())
    return names


def _has_include_line(path: Path, expected: str) -> bool:
    for line in path.read_text().splitlines():
        cleaned = _strip_inline_comment(line).strip()
        if cleaned == expected:
            return True
    return False


def main() -> int:
    missing: list[str] = []
    constraints = _parse_constraints(CONSTRAINTS)
    dev_constraints = _parse_constraints(DEV_CONSTRAINTS)

    base_requirements = _parse_requirements(REQUIREMENTS_IN)
    for requirement in sorted(base_requirements):
        if requirement not in constraints:
            missing.append(f"{CONSTRAINTS}: missing pin for {requirement}")

    dev_requirements = _parse_requirements(REQUIREMENTS_DEV_IN)
    dev_only = sorted(dev_requirements - base_requirements)
    for requirement in dev_only:
        if requirement not in dev_constraints:
            missing.append(f"{DEV_CONSTRAINTS}: missing pin for {requirement}")

    if not _has_include_line(DEV_CONSTRAINTS, "-r requirements.constraints.txt"):
        missing.append(
            f"{DEV_CONSTRAINTS}: missing '-r requirements.constraints.txt' include"
        )

    if missing:
        print("Lockfile validation failed:")
        for item in missing:
            print(f" - {item}")
        return 1

    print("Lockfile validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
