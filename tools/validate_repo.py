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
import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.strategy_registry import normalize_download, validate_download_config

EXPECTED_TARGETS_SCHEMA = "0.8"


def read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def iter_targets_files(root: Path) -> Iterable[Path]:
    for pipeline_dir in sorted(root.glob("*_pipeline_v2")):
        if not pipeline_dir.is_dir():
            continue
        yield from sorted(pipeline_dir.glob("targets_*.yaml"))


def load_profiles(license_map_path: Path) -> dict[str, Any]:
    if not license_map_path.exists():
        return {}
    data = read_yaml(license_map_path)
    return data.get("profiles", {}) or data.get("license_profiles", {}) or {}

def _parse_updated_utc(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _collect_updated_utc_warnings(path: Path) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    cfg = read_yaml(path) or {}
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


def validate_targets_file(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    cfg = read_yaml(path) or {}
    warnings.extend(_collect_updated_utc_warnings(path))
    schema_version = str(cfg.get("schema_version", ""))
    if schema_version and schema_version != EXPECTED_TARGETS_SCHEMA:
        errors.append({
            "type": "schema_version_mismatch",
            "targets_path": str(path),
            "expected": EXPECTED_TARGETS_SCHEMA,
            "found": schema_version,
        })

    companion = cfg.get("companion_files", {}) or {}
    license_map_path = Path(companion.get("license_map", "license_map.yaml"))
    if not license_map_path.is_absolute():
        license_map_path = path.parent / license_map_path
    if license_map_path.exists():
        warnings.extend(_collect_updated_utc_warnings(license_map_path))
    profiles = load_profiles(license_map_path)

    field_schemas_path = Path(companion.get("field_schemas", "field_schemas.yaml"))
    if not field_schemas_path.is_absolute():
        field_schemas_path = path.parent / field_schemas_path
    if field_schemas_path.exists():
        warnings.extend(_collect_updated_utc_warnings(field_schemas_path))

    denylist_path = Path(companion.get("denylist", "denylist.yaml"))
    if not denylist_path.is_absolute():
        denylist_path = path.parent / denylist_path
    if denylist_path.exists():
        warnings.extend(_collect_updated_utc_warnings(denylist_path))

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
        requirement_errors = validate_download_config(download, strategy)
        for msg in requirement_errors:
            error_type = "unknown_download_strategy" if msg.startswith("unknown strategy") else "missing_download_requirement"
            errors.append({
                "type": error_type,
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

    output_path = None
    if args.output:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = root / output_path
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    summary = {
        "errors": len(report["errors"]),
        "warnings": len(report["warnings"]),
        "report_path": str(output_path) if output_path else None,
    }
    print(json.dumps(summary, indent=2))

    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
