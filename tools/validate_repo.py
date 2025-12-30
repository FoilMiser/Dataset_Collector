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
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml

EXPECTED_TARGETS_SCHEMA = "0.8"


def read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def iter_targets_files(root: Path) -> Iterable[Path]:
    for pipeline_dir in sorted(root.glob("*_pipeline_v2")):
        if not pipeline_dir.is_dir():
            continue
        for target in sorted(pipeline_dir.glob("targets_*.yaml")):
            yield target


def load_profiles(license_map_path: Path) -> Dict[str, Any]:
    if not license_map_path.exists():
        return {}
    data = read_yaml(license_map_path)
    return data.get("profiles", {}) or data.get("license_profiles", {}) or {}


def get_download_requirement_errors(download: Dict[str, Any], strategy: str) -> List[str]:
    errors = []
    strategy = (strategy or "").lower()

    if strategy in {"http", "ftp"}:
        if not (download.get("url") or download.get("urls") or download.get("base_url")):
            errors.append("download.url(s) or base_url required for http/ftp strategy")
    elif strategy == "git":
        if not (download.get("repo") or download.get("repo_url") or download.get("url")):
            errors.append("download.repo/repo_url/url required for git strategy")
    elif strategy == "huggingface_datasets":
        if not download.get("dataset_id"):
            errors.append("download.dataset_id required for huggingface_datasets strategy")
    elif strategy == "zenodo":
        if not (download.get("record_id") or download.get("doi") or download.get("url")):
            errors.append("download.record_id/doi/url required for zenodo strategy")
    elif strategy == "dataverse":
        if not (download.get("persistent_id") or download.get("url")):
            errors.append("download.persistent_id/url required for dataverse strategy")

    return errors


def validate_targets_file(path: Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    cfg = read_yaml(path) or {}
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
    profiles = load_profiles(license_map_path)

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

        download = target.get("download", {}) or {}
        strategy = str(download.get("strategy", ""))
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

    return errors, warnings


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate Dataset Collector v2 repo configuration.")
    ap.add_argument("--root", default=".", help="Repo root (defaults to current directory)")
    ap.add_argument("--output", default="validation_report.json", help="Output report path")
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

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = root / output_path
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    summary = {
        "errors": len(report["errors"]),
        "warnings": len(report["warnings"]),
        "report_path": str(output_path),
    }
    print(json.dumps(summary, indent=2))

    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
