"""Schema version helpers for Dataset Collector configs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from collector_core.exceptions import ConfigValidationError

SUPPORTED_SCHEMA_VERSIONS: dict[str, set[str]] = {
    "targets": {"0.9"},
    "license_map": {"0.9"},
    "denylist": {"0.9", "1.0"},
    "field_schemas": {"0.9"},
}

MIGRATION_GUIDANCE: dict[str, str] = {
    "targets": "See docs/migration_guide.md for targets.yaml updates.",
    "license_map": "See docs/migration_guide.md for license_map.yaml updates.",
    "denylist": "See docs/migration_guide.md for denylist.yaml updates.",
    "field_schemas": "See docs/migration_guide.md for field_schemas.yaml updates.",
}


@dataclass(frozen=True)
class SchemaCompatibility:
    schema_name: str
    declared_version: str | None
    supported_versions: set[str]
    is_supported: bool
    migration_hint: str | None


def get_schema_version(config: Any) -> str | None:
    if isinstance(config, dict):
        value = config.get("schema_version")
        if value is None:
            return None
        return str(value)
    return None


def check_schema_compatibility(schema_name: str, config: Any) -> SchemaCompatibility:
    supported = SUPPORTED_SCHEMA_VERSIONS.get(schema_name, set())
    declared = get_schema_version(config)
    is_supported = bool(declared and declared in supported) or not supported
    migration_hint = None
    if supported and (declared is None or declared not in supported):
        migration_hint = MIGRATION_GUIDANCE.get(schema_name)
    return SchemaCompatibility(
        schema_name=schema_name,
        declared_version=declared,
        supported_versions=supported,
        is_supported=is_supported,
        migration_hint=migration_hint,
    )


def validate_schema_version(schema_name: str, config: Any) -> None:
    compatibility = check_schema_compatibility(schema_name, config)
    if compatibility.is_supported:
        return
    supported = ", ".join(sorted(compatibility.supported_versions)) or "<unknown>"
    declared = compatibility.declared_version or "<missing>"
    message = (
        f"Unsupported schema_version for {schema_name}: {declared}. "
        f"Supported versions: {supported}."
    )
    if compatibility.migration_hint:
        message = f"{message} {compatibility.migration_hint}"
    raise ConfigValidationError(message)
