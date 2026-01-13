#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path

from collector_core.config_validator import Draft7Validator, read_yaml
from collector_core.exceptions import ConfigValidationError, YamlParseError
from collector_core.targets_paths import list_targets_files


def iter_targets_files(root: Path) -> Iterable[Path]:
    yield from list_targets_files(root)


def _resolve_companion_paths(targets_path: Path, value: object, default: str) -> list[Path]:
    raw = value if value not in (None, "") else default
    if isinstance(raw, (list, tuple)):
        entries = raw
    else:
        entries = [raw]
    paths: list[Path] = []
    for entry in entries:
        if entry in (None, ""):
            continue
        path = Path(str(entry))
        if not path.is_absolute():
            path = targets_path.parent / path
        paths.append(path)
    return paths


def iter_companion_files(root: Path) -> Iterable[tuple[Path, str]]:
    seen: set[tuple[Path, str]] = set()
    for targets_path in iter_targets_files(root):
        try:
            cfg = read_yaml(targets_path, schema_name="targets") or {}
        except (ConfigValidationError, YamlParseError, FileNotFoundError):
            continue
        companion = cfg.get("companion_files", {}) or {}
        for name, schema, default in (
            ("license_map", "license_map", "license_map.yaml"),
            ("field_schemas", "field_schemas", "field_schemas.yaml"),
            ("denylist", "denylist", "denylist.yaml"),
        ):
            for path in _resolve_companion_paths(targets_path, companion.get(name), default):
                key = (path, schema)
                if key in seen:
                    continue
                seen.add(key)
                if path.exists():
                    yield path, schema


def iter_pipeline_maps(root: Path) -> Iterable[Path]:
    tools_dir = root / "src" / "tools"
    if not tools_dir.is_dir():
        return
    yield from sorted(tools_dir.glob("pipeline_map*.yaml"))


def validate_file(path: Path, schema_name: str) -> list[str]:
    try:
        read_yaml(path, schema_name=schema_name)
        return []
    except (ConfigValidationError, YamlParseError, FileNotFoundError) as exc:
        return [f"{path}: {exc}"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate checked-in YAML files against JSON schemas.")
    ap.add_argument("--root", default=".", help="Repository root")
    args = ap.parse_args(argv)

    if Draft7Validator is None:
        print("Install jsonschema for full schema validation. Running version checks only.")

    root = Path(args.root).resolve()
    errors: list[str] = []

    for path in iter_targets_files(root):
        errors.extend(validate_file(path, "targets"))

    for path, schema in iter_companion_files(root):
        errors.extend(validate_file(path, schema))

    for path in iter_pipeline_maps(root):
        errors.extend(validate_file(path, "pipeline_map"))

    if errors:
        print("YAML schema validation failures:")
        for err in errors:
            print(f"- {err}")
        return 1

    print("YAML schema validation succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
