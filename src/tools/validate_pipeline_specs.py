#!/usr/bin/env python3
"""
tools/validate_pipeline_specs.py

Validates that pipeline specs loaded from configs/pipelines.yaml are complete
and consistent. This tool is intended to be run in CI to catch configuration
errors early.

Checks performed:
1. All required fields are present in YAML
2. Target files referenced in YAML exist
3. Routing configuration is valid
4. Domain names are consistent
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any


def _get_project_root() -> Path:
    """Get the project root directory."""
    # From src/tools, go up two levels
    return Path(__file__).parent.parent.parent


def _load_yaml() -> dict[str, Any]:
    """Load pipelines.yaml."""
    import yaml

    yaml_path = _get_project_root() / "configs" / "pipelines.yaml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"pipelines.yaml not found at {yaml_path}")

    with open(yaml_path) as f:
        return yaml.safe_load(f)


def validate_pipeline_config(domain: str, config: dict[str, Any], project_root: Path) -> list[str]:
    """Validate a single pipeline configuration. Returns list of errors."""
    errors: list[str] = []

    # Check required fields
    if "pipeline_id" not in config:
        errors.append(f"[{domain}] Missing required field: pipeline_id")

    if "targets_path" not in config:
        errors.append(f"[{domain}] Missing required field: targets_path")
    else:
        # Check that target file exists
        targets_path = project_root / config["targets_path"]
        if not targets_path.exists():
            errors.append(f"[{domain}] Target file not found: {config['targets_path']}")

    # Validate routing configuration
    routing = config.get("routing", {})
    if not routing:
        errors.append(f"[{domain}] Missing routing configuration")
    else:
        if "keys" not in routing or not routing["keys"]:
            errors.append(f"[{domain}] Missing or empty routing.keys")
        if "confidence_keys" not in routing or not routing["confidence_keys"]:
            errors.append(f"[{domain}] Missing or empty routing.confidence_keys")
        if "default" not in routing:
            errors.append(f"[{domain}] Missing routing.default")
        else:
            default = routing["default"]
            required_default_fields = ["subject", "domain", "category", "level", "granularity"]
            for field in required_default_fields:
                if field not in default:
                    errors.append(f"[{domain}] Missing routing.default.{field}")

    # Validate knobs if present
    knobs = config.get("knobs", {})
    if "custom_workers" in knobs:
        if not isinstance(knobs["custom_workers"], dict):
            errors.append(f"[{domain}] knobs.custom_workers must be a dict")

    # Validate yellow_screen if present
    if "yellow_screen" in config:
        yellow = config["yellow_screen"]
        # yellow_screen should be a simple string (screen name)
        if not isinstance(yellow, str):
            errors.append(f"[{domain}] yellow_screen must be a string")

    return errors


def validate_all_pipelines(verbose: bool = False) -> tuple[int, list[str]]:
    """Validate all pipeline configurations. Returns (error_count, error_messages)."""
    project_root = _get_project_root()
    data = _load_yaml()
    pipelines = data.get("pipelines", {})

    all_errors: list[str] = []
    validated_count = 0

    for domain, config in pipelines.items():
        errors = validate_pipeline_config(domain, config, project_root)
        if errors:
            all_errors.extend(errors)
        validated_count += 1
        if verbose and not errors:
            print(f"  ✓ {domain}")

    return len(all_errors), all_errors


def validate_loader_consistency(verbose: bool = False) -> tuple[int, list[str]]:
    """Validate that the loader produces consistent results."""
    from collector_core.pipeline_specs_loader import load_pipeline_specs_from_yaml

    errors: list[str] = []

    try:
        specs = load_pipeline_specs_from_yaml()
        if verbose:
            print(f"  Loaded {len(specs)} pipeline specs from YAML")

        # Check that all specs have required attributes
        for domain, spec in specs.items():
            if not spec.domain:
                errors.append(f"[{domain}] Loaded spec has empty domain")
            if not spec.name:
                errors.append(f"[{domain}] Loaded spec has empty name")
            if not spec.targets_yaml:
                errors.append(f"[{domain}] Loaded spec has empty targets_yaml")
            if not spec.routing_keys:
                errors.append(f"[{domain}] Loaded spec has empty routing_keys")

    except Exception as e:
        errors.append(f"Failed to load specs from YAML: {e}")

    return len(errors), errors


def main() -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Validate pipeline specs in configs/pipelines.yaml"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show detailed validation output"
    )
    parser.add_argument(
        "--check-loader",
        action="store_true",
        help="Also validate that the loader produces consistent results"
    )
    args = parser.parse_args()

    print("Validating pipeline specs...")

    # Validate YAML configuration
    print("\n1. Validating YAML configuration:")
    yaml_errors, yaml_messages = validate_all_pipelines(verbose=args.verbose)

    if yaml_errors:
        print(f"\n  ✗ Found {yaml_errors} error(s) in YAML:")
        for msg in yaml_messages:
            print(f"    - {msg}")
    else:
        print("  ✓ All pipeline YAML configurations are valid")

    total_errors = yaml_errors

    # Optionally validate loader consistency
    if args.check_loader:
        print("\n2. Validating loader consistency:")
        loader_errors, loader_messages = validate_loader_consistency(verbose=args.verbose)

        if loader_errors:
            print(f"\n  ✗ Found {loader_errors} error(s) in loader:")
            for msg in loader_messages:
                print(f"    - {msg}")
        else:
            print("  ✓ Loader produces consistent results")

        total_errors += loader_errors

    # Summary
    print(f"\n{'=' * 50}")
    if total_errors:
        print(f"FAILED: {total_errors} error(s) found")
        return 1
    else:
        print("SUCCESS: All pipeline specs are valid")
        return 0


if __name__ == "__main__":
    sys.exit(main())
