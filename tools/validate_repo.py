#!/usr/bin/env python3
"""Validate repository structure and configuration.

This script checks that the repository follows the expected structure
and configuration requirements:
- Required files exist (pyproject.toml, README.md, etc.)
- Source tree is valid
- No broken symlinks
- Package is importable
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def check_required_files(root: Path) -> list[str]:
    """Check that required files exist."""
    required_files = [
        "pyproject.toml",
        "README.md",
        "LICENSE",
        "requirements.in",
        "src/collector_core/__init__.py",
    ]

    missing = []
    for file_path in required_files:
        if not (root / file_path).exists():
            missing.append(file_path)

    return missing


def check_broken_symlinks(root: Path) -> list[Path]:
    """Find broken symlinks in the repository."""
    broken: list[Path] = []

    for path in root.rglob("*"):
        if path.is_symlink() and not path.exists():
            broken.append(path.relative_to(root))

    return broken


def check_package_importable() -> tuple[bool, str]:
    """Check that the package can be imported."""
    try:
        import collector_core  # noqa: F401

        return True, ""
    except ImportError as e:
        return False, str(e)


def check_no_deprecated_imports(root: Path) -> list[str]:
    """Check for deprecated imports that shouldn't be used."""
    deprecated_patterns = [
        "from collector_core.legacy",
        "import collector_core.legacy",
    ]

    issues = []
    src_dir = root / "src"

    if not src_dir.exists():
        return issues

    for py_file in src_dir.rglob("*.py"):
        try:
            content = py_file.read_text()
            for pattern in deprecated_patterns:
                if pattern in content:
                    issues.append(f"{py_file.relative_to(root)}: uses deprecated import '{pattern}'")
        except (OSError, UnicodeDecodeError):
            pass

    return issues


def main() -> int:
    """Run repository validation."""
    parser = argparse.ArgumentParser(description="Validate repository structure")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="Repository root")
    args = parser.parse_args()

    root = args.root.resolve()
    errors: list[str] = []

    print(f"Validating repository at {root}")
    print("-" * 60)

    # Check required files
    missing_files = check_required_files(root)
    if missing_files:
        for f in missing_files:
            errors.append(f"Missing required file: {f}")
        print(f"[FAIL] Missing {len(missing_files)} required file(s)")
    else:
        print("[PASS] All required files present")

    # Check for broken symlinks
    broken_symlinks = check_broken_symlinks(root)
    if broken_symlinks:
        for link in broken_symlinks:
            errors.append(f"Broken symlink: {link}")
        print(f"[FAIL] Found {len(broken_symlinks)} broken symlink(s)")
    else:
        print("[PASS] No broken symlinks")

    # Check package importability
    can_import, import_error = check_package_importable()
    if not can_import:
        errors.append(f"Cannot import collector_core: {import_error}")
        print(f"[FAIL] Package not importable: {import_error}")
    else:
        print("[PASS] Package is importable")

    # Check for deprecated imports
    deprecated = check_no_deprecated_imports(root)
    if deprecated:
        for issue in deprecated:
            errors.append(issue)
        print(f"[FAIL] Found {len(deprecated)} deprecated import(s)")
    else:
        print("[PASS] No deprecated imports")

    print("-" * 60)

    if errors:
        print(f"\nValidation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("\nValidation passed!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
