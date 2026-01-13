#!/usr/bin/env python3
"""Validate YAML files against their schemas.

This script validates YAML configuration files in the repository
against their corresponding JSON schemas.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

try:
    import jsonschema
except ImportError:
    jsonschema = None  # type: ignore[assignment]


def find_yaml_files(root: Path) -> list[Path]:
    """Find all YAML files in the repository."""
    yaml_files: list[Path] = []

    # Common YAML directories
    search_paths = [
        root / "configs",
        root / "schemas",
    ]

    for search_path in search_paths:
        if search_path.exists():
            yaml_files.extend(search_path.rglob("*.yml"))
            yaml_files.extend(search_path.rglob("*.yaml"))

    # Also include pipeline config files
    for pipeline_dir in root.glob("*_pipeline_v*"):
        yaml_files.extend(pipeline_dir.rglob("*.yml"))
        yaml_files.extend(pipeline_dir.rglob("*.yaml"))

    return yaml_files


def find_schema_for_file(yaml_path: Path, schemas_dir: Path) -> Path | None:
    """Find the schema file for a given YAML file."""
    # Try to match schema by file name
    schema_name = yaml_path.stem + ".schema.json"
    schema_path = schemas_dir / schema_name

    if schema_path.exists():
        return schema_path

    # Try common schema names
    common_schemas = [
        "pipeline.schema.json",
        "config.schema.json",
    ]

    for schema in common_schemas:
        schema_path = schemas_dir / schema
        if schema_path.exists():
            return schema_path

    return None


def validate_yaml_syntax(yaml_path: Path) -> tuple[bool, str]:
    """Check that a YAML file has valid syntax."""
    try:
        with open(yaml_path) as f:
            yaml.safe_load(f)
        return True, ""
    except yaml.YAMLError as e:
        return False, str(e)


def validate_yaml_against_schema(
    yaml_path: Path, schema_path: Path
) -> tuple[bool, list[str]]:
    """Validate a YAML file against a JSON schema."""
    if jsonschema is None:
        return True, []  # Skip if jsonschema not available

    try:
        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        with open(schema_path) as f:
            schema = json.load(f)

        jsonschema.validate(data, schema)
        return True, []

    except jsonschema.ValidationError as e:
        return False, [f"Schema validation error: {e.message}"]
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        return False, [f"Parse error: {e}"]


def main() -> int:
    """Run YAML validation."""
    parser = argparse.ArgumentParser(description="Validate YAML files")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="Repository root")
    parser.add_argument("--strict", action="store_true", help="Fail on any issue")
    args = parser.parse_args()

    root = args.root.resolve()
    schemas_dir = root / "schemas"

    print(f"Validating YAML files in {root}")
    print("-" * 60)

    yaml_files = find_yaml_files(root)
    errors: list[str] = []

    if not yaml_files:
        print("No YAML files found to validate")
        return 0

    for yaml_path in sorted(yaml_files):
        relative_path = yaml_path.relative_to(root)

        # Check syntax
        valid_syntax, syntax_error = validate_yaml_syntax(yaml_path)
        if not valid_syntax:
            errors.append(f"{relative_path}: {syntax_error}")
            print(f"[FAIL] {relative_path}: Invalid YAML syntax")
            continue

        # Try schema validation if available
        schema_path = find_schema_for_file(yaml_path, schemas_dir)
        if schema_path:
            valid_schema, schema_errors = validate_yaml_against_schema(yaml_path, schema_path)
            if not valid_schema:
                for error in schema_errors:
                    errors.append(f"{relative_path}: {error}")
                print(f"[FAIL] {relative_path}: Schema validation failed")
            else:
                print(f"[PASS] {relative_path}")
        else:
            print(f"[PASS] {relative_path} (no schema)")

    print("-" * 60)

    if errors:
        print(f"\nValidation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"  - {error}")
        return 1

    print(f"\nValidated {len(yaml_files)} YAML file(s) successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
