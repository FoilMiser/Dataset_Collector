#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from pathlib import Path

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from collector_core.config_validator import read_yaml
from collector_core.exceptions import ConfigValidationError, YamlParseError


def iter_targets_files(root: Path) -> Iterable[Path]:
    for pipeline_dir in sorted(root.glob("*_pipeline_v2")):
        if not pipeline_dir.is_dir():
            continue
        yield from sorted(pipeline_dir.glob("targets_*.yaml"))


def iter_companion_files(root: Path) -> Iterable[tuple[Path, str]]:
    for pipeline_dir in sorted(root.glob("*_pipeline_v2")):
        if not pipeline_dir.is_dir():
            continue
        for name, schema in (
            ("license_map.yaml", "license_map"),
            ("field_schemas.yaml", "field_schemas"),
            ("denylist.yaml", "denylist"),
        ):
            path = pipeline_dir / name
            if path.exists():
                yield path, schema


def iter_pipeline_maps(root: Path) -> Iterable[Path]:
    tools_dir = root / "tools"
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
