from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from collector_core.config_validator import read_yaml


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
        path = Path(str(entry)).expanduser()
        if not path.is_absolute():
            path = (targets_path.parent / path).resolve()
        else:
            path = path.resolve()
        paths.append(path)
    return paths


def _extend_unique(items: list[Any], additions: Sequence[Any]) -> None:
    for item in additions:
        if item not in items:
            items.append(item)


def read_license_maps(paths: Sequence[Path]) -> dict[str, Any]:
    merged = {
        "spdx": {"allow": [], "conditional": [], "deny_prefixes": []},
        "normalization": {"rules": []},
        "restriction_scan": {"phrases": []},
        "gating": {},
        "profiles": {},
        "evidence_change_policy": None,
        "cosmetic_change_policy": None,
    }
    for path in paths:
        data = read_yaml(path, schema_name="license_map") or {}
        spdx = data.get("spdx", {}) or {}
        _extend_unique(merged["spdx"]["allow"], spdx.get("allow", []) or [])
        _extend_unique(merged["spdx"]["conditional"], spdx.get("conditional", []) or [])
        _extend_unique(merged["spdx"]["deny_prefixes"], spdx.get("deny_prefixes", []) or [])

        normalization = data.get("normalization", {}) or {}
        _extend_unique(merged["normalization"]["rules"], normalization.get("rules", []) or [])

        restriction_scan = data.get("restriction_scan", {}) or {}
        _extend_unique(
            merged["restriction_scan"]["phrases"], restriction_scan.get("phrases", []) or []
        )

        gating = data.get("gating", {}) or {}
        merged["gating"].update(gating)

        profiles = data.get("profiles", {}) or data.get("license_profiles", {}) or {}
        merged["profiles"].update(profiles)

        if data.get("evidence_change_policy") is not None:
            merged["evidence_change_policy"] = data.get("evidence_change_policy")
        if data.get("cosmetic_change_policy") is not None:
            merged["cosmetic_change_policy"] = data.get("cosmetic_change_policy")
    return merged


def read_field_schemas(paths: Sequence[Path]) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for path in paths:
        if not path.exists():
            continue
        data = read_yaml(path, schema_name="field_schemas") or {}
        schemas = data.get("schemas", {}) or {}
        merged.update(schemas)
    return merged


def read_denylist_raw(paths: Sequence[Path]) -> dict[str, Any]:
    merged = {"patterns": [], "domain_patterns": [], "publisher_patterns": []}
    for path in paths:
        if not path.exists():
            continue
        data = read_yaml(path, schema_name="denylist") or {}
        _extend_unique(merged["patterns"], data.get("patterns", []) or [])
        _extend_unique(merged["domain_patterns"], data.get("domain_patterns", []) or [])
        _extend_unique(merged["publisher_patterns"], data.get("publisher_patterns", []) or [])
    return merged
