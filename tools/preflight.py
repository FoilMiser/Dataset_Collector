#!/usr/bin/env python3
"""Preflight checks for Dataset Collector.

This script runs essential checks before deploying or using the
Dataset Collector in production:
- Package imports correctly
- All pipelines are discoverable
- Configuration is valid
- Dependencies are available
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def check_imports(quiet: bool = False) -> tuple[bool, list[str]]:
    """Check that essential modules can be imported."""
    modules = [
        "collector_core",
        "collector_core.dc_cli",
        "collector_core.pipeline_factory",
        "collector_core.acquire.strategies.registry",
        "collector_core.yellow.base",
        "collector_core.checks.registry",
    ]

    errors: list[str] = []

    for module in modules:
        try:
            __import__(module)
            if not quiet:
                print(f"  [OK] {module}")
        except ImportError as e:
            errors.append(f"Cannot import {module}: {e}")
            if not quiet:
                print(f"  [FAIL] {module}: {e}")

    return len(errors) == 0, errors


def check_pipelines(root: Path, quiet: bool = False) -> tuple[bool, list[str]]:
    """Check that pipeline directories exist and have config files."""
    pipeline_dirs = list(root.glob("*_pipeline_v*"))
    errors: list[str] = []

    if not pipeline_dirs:
        errors.append("No pipeline directories found")
        return False, errors

    for pipeline_dir in sorted(pipeline_dirs):
        config_files = list(pipeline_dir.glob("*.yml")) + list(pipeline_dir.glob("*.yaml"))
        if not config_files:
            errors.append(f"{pipeline_dir.name}: No config files found")
            if not quiet:
                print(f"  [WARN] {pipeline_dir.name}: No config files")
        else:
            if not quiet:
                print(f"  [OK] {pipeline_dir.name}: {len(config_files)} config(s)")

    return len(errors) == 0, errors


def check_schemas(root: Path, quiet: bool = False) -> tuple[bool, list[str]]:
    """Check that schema files exist."""
    schemas_dir = root / "schemas"
    errors: list[str] = []

    if not schemas_dir.exists():
        # Try collector_core schemas
        schemas_dir = root / "src" / "collector_core" / "schemas"

    if not schemas_dir.exists():
        if not quiet:
            print("  [WARN] No schemas directory found")
        return True, []  # Not critical

    schema_files = list(schemas_dir.glob("*.json")) + list(schemas_dir.glob("*.yaml"))

    if not schema_files:
        if not quiet:
            print("  [WARN] No schema files found")
    else:
        if not quiet:
            print(f"  [OK] Found {len(schema_files)} schema file(s)")

    return True, errors


def check_dependencies(quiet: bool = False) -> tuple[bool, list[str]]:
    """Check that required dependencies are available."""
    dependencies = [
        ("yaml", "PyYAML"),
        ("requests", "requests"),
        ("jsonschema", "jsonschema"),
    ]

    errors: list[str] = []

    for module, package in dependencies:
        try:
            __import__(module)
            if not quiet:
                print(f"  [OK] {package}")
        except ImportError:
            errors.append(f"Missing dependency: {package}")
            if not quiet:
                print(f"  [FAIL] {package}: Not installed")

    return len(errors) == 0, errors


def main() -> int:
    """Run preflight checks."""
    parser = argparse.ArgumentParser(description="Preflight checks")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd(), help="Repository root")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    args = parser.parse_args()

    root = args.repo_root.resolve()
    quiet = args.quiet
    all_errors: list[str] = []

    if not quiet:
        print("Dataset Collector Preflight Checks")
        print("=" * 60)

    # Check imports
    if not quiet:
        print("\nChecking module imports...")
    ok, errors = check_imports(quiet)
    all_errors.extend(errors)

    # Check pipelines
    if not quiet:
        print("\nChecking pipelines...")
    ok, errors = check_pipelines(root, quiet)
    all_errors.extend(errors)

    # Check schemas
    if not quiet:
        print("\nChecking schemas...")
    ok, errors = check_schemas(root, quiet)
    all_errors.extend(errors)

    # Check dependencies
    if not quiet:
        print("\nChecking dependencies...")
    ok, errors = check_dependencies(quiet)
    all_errors.extend(errors)

    if not quiet:
        print("\n" + "=" * 60)

    if all_errors:
        if not quiet:
            print(f"\nPreflight failed with {len(all_errors)} issue(s):")
            for error in all_errors:
                print(f"  - {error}")
        return 1

    if not quiet:
        print("\nPreflight checks passed!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
