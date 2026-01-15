from __future__ import annotations

import hashlib
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from collector_core.__version__ import __schema_version__ as SCHEMA_VERSION
from collector_core.__version__ import __version__ as VERSION
from collector_core.config_validator import read_yaml
from collector_core.utils.logging import utc_now


def _resolve_git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        # P1.1B: Catch specific subprocess/file errors
        return "unknown"
    sha = (result.stdout or "").strip()
    return sha or "unknown"


def _hash_paths(paths: Sequence[Path]) -> str | None:
    hasher = hashlib.sha256()
    found = False
    for path in sorted(paths, key=lambda p: str(p)):
        if not path.exists():
            continue
        found = True
        hasher.update(str(path).encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(path.read_bytes())
        hasher.update(b"\0")
    if not found:
        return None
    return hasher.hexdigest()


def _collect_schema_versions(paths: Sequence[Path], schema_name: str) -> list[str]:
    versions: list[str] = []
    for path in sorted(paths, key=lambda p: str(p)):
        if not path.exists():
            continue
        data = read_yaml(path, schema_name=schema_name) or {}
        version = str(data.get("schema_version", "") or "").strip()
        if version and version not in versions:
            versions.append(version)
    return versions


def _merge_override(default_values: list[str], overrides: dict[str, Any]) -> list[str]:
    merged = list(default_values)
    add = overrides.get("add", []) or []
    remove = overrides.get("remove", []) or []

    for item in add:
        if item not in merged:
            merged.append(item)
    for item in remove:
        if item in merged:
            merged.remove(item)
    return merged


def _canonicalize_checks(checks: list[str]) -> list[str]:
    canonicalized: list[str] = []
    for check in checks:
        if check not in canonicalized:
            canonicalized.append(check)
    return canonicalized


def _resolve_enabled_checks(
    *, default_content_checks: list[str], targets: list[dict[str, Any]]
) -> list[str]:
    enabled_checks: list[str] = []
    for target in targets:
        if not target.get("enabled", True):
            continue
        merged = _merge_override(
            default_content_checks, target.get("content_checks", {}) or {}
        )
        for check in _canonicalize_checks(merged):
            if check not in enabled_checks:
                enabled_checks.append(check)
    return enabled_checks


def build_policy_snapshot(
    *,
    run_id: str,
    targets_path: Path,
    targets_cfg: dict[str, Any],
    license_map_paths: Sequence[Path],
    denylist_paths: Sequence[Path],
    default_content_checks: list[str],
    targets: list[dict[str, Any]],
) -> dict[str, Any]:
    targets_schema_version = str(targets_cfg.get("schema_version", SCHEMA_VERSION) or "")
    license_versions = _collect_schema_versions(license_map_paths, "license_map")
    denylist_versions = _collect_schema_versions(denylist_paths, "denylist")

    return {
        "run_id": run_id,
        "git_sha": _resolve_git_sha(),
        "core_version": VERSION,
        "license_map_hash": _hash_paths(license_map_paths),
        "denylist_hash": _hash_paths(denylist_paths),
        "schema_versions": {
            "targets": targets_schema_version,
            "license_map": license_versions,
            "denylist": denylist_versions,
        },
        "enabled_checks": _resolve_enabled_checks(
            default_content_checks=default_content_checks, targets=targets
        ),
        "generated_at_utc": utc_now(),
    }
