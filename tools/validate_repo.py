#!/usr/bin/env python3
"""Repo-level validator for Dataset Collector v2.

Validates enabled targets across all pipeline configs:
- license evidence URL present
- download config satisfies strategy requirements
- license_profile exists in license_map profiles
- review_required implies review_notes
- targets schema_version matches expected
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from collector_core.config_validator import read_yaml
from collector_core.exceptions import ConfigValidationError, YamlParseError
from tools.strategy_registry import (
    get_strategy_requirement_errors,
    get_strategy_spec,
    iter_registry_strategies,
)

EXPECTED_TARGETS_SCHEMA = "0.9"


def normalize_download(download: dict[str, Any]) -> dict[str, Any]:
    d = dict(download or {})
    cfg = d.get("config")

    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in d.items() if k != "config"})
        d = merged

    if d.get("strategy") == "zenodo":
        if not d.get("record_id") and d.get("record"):
            d["record_id"] = d["record"]
        if not d.get("record_id") and isinstance(d.get("record_ids"), list) and d["record_ids"]:
            d["record_id"] = d["record_ids"][0]

    return d


def iter_targets_files(root: Path) -> Iterable[Path]:
    for pipeline_dir in sorted(root.glob("*_pipeline_v2")):
        if not pipeline_dir.is_dir():
            continue
        yield from sorted(pipeline_dir.glob("targets_*.yaml"))


def iter_pipeline_drivers(pipeline_dirs: Iterable[Path]) -> Iterable[Path]:
    for pipeline_dir in pipeline_dirs:
        driver = pipeline_dir / "pipeline_driver.py"
        if driver.exists():
            yield driver


def iter_acquire_workers(root: Path) -> Iterable[Path]:
    for pipeline_dir in sorted(root.glob("*_pipeline_v2")):
        if not pipeline_dir.is_dir():
            continue
        worker = pipeline_dir / "acquire_worker.py"
        if worker.exists():
            yield worker


def iter_yellow_scrubbers(root: Path) -> Iterable[Path]:
    for pipeline_dir in sorted(root.glob("*_pipeline_v2")):
        if not pipeline_dir.is_dir():
            continue
        yield from sorted(pipeline_dir.glob("yellow_scrubber*.py"))


def iter_collector_core_versioned_modules(root: Path) -> Iterable[Path]:
    core_dir = root / "collector_core"
    if not core_dir.is_dir():
        return
    yield from sorted(core_dir.glob("yellow_screen_*.py"))
    merge = core_dir / "merge.py"
    if merge.exists():
        yield merge
    pipeline_version = core_dir / "pipeline_version.py"
    if pipeline_version.exists():
        yield pipeline_version


READ_YAML_EXCEPTIONS = (
    ConfigValidationError,
    YamlParseError,
    FileNotFoundError,
    OSError,
    ValueError,
)


def _append_read_error(errors: list[dict[str, Any]], path: Path, exc: Exception) -> None:
    errors.append({
        "type": "read_yaml_error",
        "path": str(path),
        "exception": exc.__class__.__name__,
        "message": str(exc),
    })


def _load_pipeline_map(
    root: Path,
    errors: list[dict[str, Any]],
) -> tuple[Path, dict[str, Any]] | None:
    pipeline_map_path = root / "tools" / "pipeline_map.yaml"
    if not pipeline_map_path.exists():
        return None
    try:
        pipeline_map = read_yaml(pipeline_map_path, schema_name="pipeline_map") or {}
    except READ_YAML_EXCEPTIONS as exc:
        _append_read_error(errors, pipeline_map_path, exc)
        return None
    return pipeline_map_path, pipeline_map


def validate_pipeline_layout(root: Path) -> tuple[list[dict[str, Any]], list[Path]]:
    errors: list[dict[str, Any]] = []
    pipeline_dirs: list[Path] = []

    pipeline_map_entry = _load_pipeline_map(root, errors)
    if pipeline_map_entry is not None:
        pipeline_map_path, pipeline_map = pipeline_map_entry
        pipelines_cfg = pipeline_map.get("pipelines", {}) or {}
        for pipeline_name, pipeline_entry in pipelines_cfg.items():
            pipeline_dir = root / pipeline_name
            if not pipeline_dir.is_dir():
                errors.append({
                    "type": "missing_pipeline_directory",
                    "pipeline": pipeline_name,
                    "path": str(pipeline_dir),
                    "pipeline_map": str(pipeline_map_path),
                })
                continue
            pipeline_dirs.append(pipeline_dir)
            required_files = [
                pipeline_dir / "pipeline_driver.py",
                pipeline_dir / "acquire_worker.py",
            ]
            for required_path in required_files:
                if not required_path.exists():
                    errors.append({
                        "type": "missing_pipeline_file",
                        "pipeline": pipeline_name,
                        "path": str(required_path),
                    })
            targets_yaml = pipeline_entry.get("targets_yaml")
            if not targets_yaml:
                errors.append({
                    "type": "missing_pipeline_targets_yaml",
                    "pipeline": pipeline_name,
                    "pipeline_map": str(pipeline_map_path),
                })
            else:
                targets_path = pipeline_dir / targets_yaml
                if not targets_path.exists():
                    errors.append({
                        "type": "missing_pipeline_targets_file",
                        "pipeline": pipeline_name,
                        "path": str(targets_path),
                    })
    else:
        for pipeline_dir in sorted(root.glob("*_pipeline_v2")):
            if not pipeline_dir.is_dir():
                continue
            pipeline_dirs.append(pipeline_dir)
            required_files = [
                pipeline_dir / "pipeline_driver.py",
                pipeline_dir / "acquire_worker.py",
            ]
            for required_path in required_files:
                if not required_path.exists():
                    errors.append({
                        "type": "missing_pipeline_file",
                        "pipeline": pipeline_dir.name,
                        "path": str(required_path),
                    })
            targets_files = list(pipeline_dir.glob("targets_*.yaml"))
            if not targets_files:
                errors.append({
                    "type": "missing_pipeline_targets_file",
                    "pipeline": pipeline_dir.name,
                    "path": str(pipeline_dir),
                })

    return errors, pipeline_dirs


def validate_pipeline_driver_versions(pipeline_dirs: Iterable[Path]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    drivers = list(iter_pipeline_drivers(pipeline_dirs))

    version_assignment = re.compile(r"^\s*VERSION\s*=", re.M)
    for driver in drivers:
        text = driver.read_text(encoding="utf-8")
        if version_assignment.search(text):
            errors.append({
                "type": "hardcoded_pipeline_version",
                "path": str(driver),
                "message": "Remove VERSION assignment from pipeline drivers.",
            })
        if "collector_core.__version__" not in text:
            errors.append({
                "type": "missing_pipeline_version_import",
                "path": str(driver),
                "message": "Import VERSION from collector_core.__version__.",
            })

    return errors


def validate_versioned_modules(root: Path) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    versioned_files = [
        *iter_acquire_workers(root),
        *iter_yellow_scrubbers(root),
        *iter_collector_core_versioned_modules(root),
    ]
    if not versioned_files:
        return errors

    hardcoded_version = re.compile(r"^\s*VERSION\s*=\s*[\"']\d", re.M)
    for path in versioned_files:
        text = path.read_text(encoding="utf-8")
        if hardcoded_version.search(text):
            errors.append({
                "type": "hardcoded_version_string",
                "path": str(path),
                "message": "Remove hardcoded VERSION strings and import from collector_core.__version__.",
            })
        if "collector_core.__version__" not in text:
            errors.append({
                "type": "missing_version_import",
                "path": str(path),
                "message": "Import VERSION from collector_core.__version__.",
            })

    return errors


def resolve_companion_paths(targets_path: Path, value: Any, default: str) -> list[Path]:
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


def load_profiles(
    license_map_paths: list[Path],
    errors: list[dict[str, Any]],
) -> dict[str, Any]:
    profiles: dict[str, Any] = {}
    for license_map_path in license_map_paths:
        if not license_map_path.exists():
            continue
        try:
            data = read_yaml(license_map_path, schema_name="license_map")
        except READ_YAML_EXCEPTIONS as exc:
            _append_read_error(errors, license_map_path, exc)
            continue
        profiles.update(data.get("profiles", {}) or data.get("license_profiles", {}) or {})
    return profiles


def _parse_updated_utc(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _collect_updated_utc_warnings(
    path: Path,
    schema_name: str,
    errors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    try:
        cfg = read_yaml(path, schema_name=schema_name) or {}
    except READ_YAML_EXCEPTIONS as exc:
        _append_read_error(errors, path, exc)
        return warnings
    updated = str(cfg.get("updated_utc", "")).strip()
    if not updated:
        return warnings

    parsed = _parse_updated_utc(updated)
    if parsed is None:
        warnings.append({
            "type": "updated_utc_invalid_format",
            "path": str(path),
            "updated_utc": updated,
            "expected_format": "YYYY-MM-DD",
        })
        return warnings

    today = datetime.now(timezone.utc).date()
    if parsed.date() > today:
        warnings.append({
            "type": "updated_utc_future_date",
            "path": str(path),
            "updated_utc": updated,
            "today_utc": today.isoformat(),
        })

    return warnings


def get_download_requirement_errors(download: dict[str, Any], strategy: str) -> list[str]:
    return get_strategy_requirement_errors(download, strategy)


def _is_placeholder_strategy(strategy: str) -> bool:
    normalized = " ".join((strategy or "").strip().lower().split())
    if normalized in {"todo", "not implemented"}:
        return True
    return normalized in {"not_implemented", "not-implemented", "none"}


def validate_targets_file(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    try:
        cfg = read_yaml(path, schema_name="targets") or {}
    except READ_YAML_EXCEPTIONS as exc:
        _append_read_error(errors, path, exc)
        return errors, warnings
    warnings.extend(_collect_updated_utc_warnings(path, "targets", errors))
    schema_version = str(cfg.get("schema_version", ""))
    if schema_version and schema_version != EXPECTED_TARGETS_SCHEMA:
        errors.append({
            "type": "schema_version_mismatch",
            "targets_path": str(path),
            "expected": EXPECTED_TARGETS_SCHEMA,
            "found": schema_version,
        })

    companion = cfg.get("companion_files", {}) or {}
    license_map_paths = resolve_companion_paths(path, companion.get("license_map"), "license_map.yaml")
    for license_map_path in license_map_paths:
        if license_map_path.exists():
            warnings.extend(_collect_updated_utc_warnings(license_map_path, "license_map", errors))
    profiles = load_profiles(license_map_paths, errors)

    field_schemas_paths = resolve_companion_paths(path, companion.get("field_schemas"), "field_schemas.yaml")
    for field_schemas_path in field_schemas_paths:
        if field_schemas_path.exists():
            warnings.extend(_collect_updated_utc_warnings(field_schemas_path, "field_schemas", errors))

    denylist_paths = resolve_companion_paths(path, companion.get("denylist"), "denylist.yaml")
    for denylist_path in denylist_paths:
        if denylist_path.exists():
            warnings.extend(_collect_updated_utc_warnings(denylist_path, "denylist", errors))

    for target in cfg.get("targets", []) or []:
        if not target.get("enabled", True):
            continue
        tid = str(target.get("id", "unknown"))
        evidence = target.get("license_evidence", {}) or {}
        evidence_url = str(evidence.get("url", "")).strip()
        if not evidence_url:
            errors.append({
                "type": "missing_license_evidence_url",
                "targets_path": str(path),
                "target_id": tid,
            })

        download = normalize_download(target.get("download", {}) or {})
        strategy = str(download.get("strategy", ""))
        if not strategy:
            errors.append({
                "type": "missing_download_strategy",
                "targets_path": str(path),
                "target_id": tid,
                "message": "download.strategy is required for enabled targets",
            })
            continue
        if _is_placeholder_strategy(strategy):
            errors.append({
                "type": "todo_download_strategy",
                "targets_path": str(path),
                "target_id": tid,
                "strategy": strategy,
                "message": f"Enabled target {tid} uses TODO/not implemented strategy '{strategy}'.",
            })
            continue
        strategy_spec = get_strategy_spec(strategy)
        if strategy_spec is not None:
            status = str(strategy_spec.get("status", "supported") or "supported").strip().lower()
            if status in {"todo", "not implemented", "not_implemented", "not-implemented", "placeholder"}:
                errors.append({
                    "type": "todo_download_strategy",
                    "targets_path": str(path),
                    "target_id": tid,
                    "strategy": strategy,
                    "message": (
                        f"Enabled target {tid} uses TODO/not implemented strategy '{strategy}' "
                        f"(status: {status})."
                    ),
                })
                continue
        if strategy_spec is None:
            errors.append({
                "type": "unknown_download_strategy",
                "targets_path": str(path),
                "target_id": tid,
                "strategy": strategy,
                "known_strategies": sorted(iter_registry_strategies()),
            })
            continue
        for msg in get_download_requirement_errors(download, strategy):
            errors.append({
                "type": "missing_download_requirement",
                "targets_path": str(path),
                "target_id": tid,
                "strategy": strategy,
                "message": msg,
            })

        profile = str(target.get("license_profile", "unknown"))
        if profile and profiles and profile not in profiles:
            warnings.append({
                "type": "unknown_license_profile",
                "targets_path": str(path),
                "target_id": tid,
                "license_profile": profile,
                "known_profiles": sorted(profiles.keys()),
            })

        if target.get("review_required", False) and not str(target.get("review_notes", "")).strip():
            errors.append({
                "type": "missing_review_notes",
                "targets_path": str(path),
                "target_id": tid,
            })

        if strategy == "huggingface_datasets":
            canonicalize = target.get("canonicalize", {}) or {}
            candidates = canonicalize.get("text_field_candidates") or []
            if not candidates:
                warnings.append({
                    "type": "missing_canonicalize_text_field_candidates",
                    "targets_path": str(path),
                    "target_id": tid,
                    "strategy": strategy,
                    "message": "Add canonicalize.text_field_candidates for HF targets to improve text extraction.",
                })

    return errors, warnings


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate Dataset Collector v2 repo configuration.")
    ap.add_argument(
        "--repo-root",
        dest="root",
        default=".",
        help="Repo root (defaults to current directory)",
    )
    ap.add_argument(
        "--root",
        dest="root",
        help="Deprecated alias for --repo-root",
    )
    ap.add_argument(
        "--output",
        default=None,
        help="Output report path (optional; omit to skip writing a report file)",
    )
    ap.add_argument(
        "--strict",
        "--fail-on-warnings",
        dest="strict",
        action="store_true",
        help="Exit with non-zero status if warnings are present.",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()
    report = {
        "errors": [],
        "warnings": [],
    }

    for targets_path in iter_targets_files(root):
        errors, warnings = validate_targets_file(targets_path)
        report["errors"].extend(errors)
        report["warnings"].extend(warnings)

    layout_errors, pipeline_dirs = validate_pipeline_layout(root)
    report["errors"].extend(layout_errors)
    report["errors"].extend(validate_pipeline_driver_versions(pipeline_dirs))
    report["errors"].extend(validate_versioned_modules(root))

    output_path = None
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = root / output_path
        tmp_path = Path(f"{output_path}.tmp")
        tmp_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        tmp_path.replace(output_path)

    exit_code = 1 if report["errors"] or (args.strict and report["warnings"]) else 0
    summary = {
        "errors": len(report["errors"]),
        "warnings": len(report["warnings"]),
        "report_path": str(output_path) if output_path else None,
        "strict_mode": args.strict,
        "exit_code": exit_code,
    }
    print(json.dumps(summary, indent=2))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
