"""Schema version validation and enforcement.

This module provides schema version validation, compatibility checks,
and migration helpers for configuration files.

Example:
    # Validate targets.yaml version
    validate_schema_version(targets_config, "targets", min_version="0.9")
    
    # Check compatibility
    if not is_compatible_version("0.8", min_version="0.9"):
        raise IncompatibleVersionError("Please upgrade your targets.yaml")
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


# Current schema versions
CURRENT_VERSIONS = {
    "targets": "0.9",
    "license_map": "0.9",
    "denylist": "1.0",
    "pipeline_map": "0.9",
    "field_schemas": "1.0",
}

# Minimum supported versions
MIN_SUPPORTED_VERSIONS = {
    "targets": "0.8",
    "license_map": "0.8",
    "denylist": "1.0",
    "pipeline_map": "0.8",
    "field_schemas": "1.0",
}


class SchemaVersionError(Exception):
    """Base exception for schema version errors."""
    pass


class IncompatibleVersionError(SchemaVersionError):
    """Raised when schema version is incompatible."""
    pass


class MissingVersionError(SchemaVersionError):
    """Raised when schema version is missing."""
    pass


@dataclass
class VersionInfo:
    """Parsed version information.
    
    Attributes:
        major: Major version number
        minor: Minor version number
        patch: Patch version number (optional)
        raw: Original version string
    """
    major: int
    minor: int
    patch: int = 0
    raw: str = ""
    
    def __str__(self) -> str:
        return self.raw or f"{self.major}.{self.minor}.{self.patch}"
    
    def __lt__(self, other: "VersionInfo") -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    
    def __le__(self, other: "VersionInfo") -> bool:
        return (self.major, self.minor, self.patch) <= (other.major, other.minor, other.patch)
    
    def __gt__(self, other: "VersionInfo") -> bool:
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)
    
    def __ge__(self, other: "VersionInfo") -> bool:
        return (self.major, self.minor, self.patch) >= (other.major, other.minor, other.patch)
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VersionInfo):
            return NotImplemented
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)


def parse_version(version_str: str) -> VersionInfo:
    """Parse a version string into VersionInfo.
    
    Supports formats: "1.0", "1.0.0", "v1.0", "1.0-beta"
    
    Args:
        version_str: Version string to parse
        
    Returns:
        Parsed VersionInfo
        
    Raises:
        ValueError: If version string cannot be parsed
    """
    if not version_str:
        raise ValueError("Empty version string")
    
    # Remove leading 'v' if present
    clean = version_str.lstrip("v").strip()
    
    # Extract version numbers
    match = re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?", clean)
    if not match:
        raise ValueError(f"Invalid version format: {version_str}")
    
    major = int(match.group(1))
    minor = int(match.group(2))
    patch = int(match.group(3)) if match.group(3) else 0
    
    return VersionInfo(major=major, minor=minor, patch=patch, raw=version_str)


def is_compatible_version(
    version: str,
    *,
    min_version: str | None = None,
    max_version: str | None = None,
) -> bool:
    """Check if version is within compatible range.
    
    Args:
        version: Version to check
        min_version: Minimum required version (inclusive)
        max_version: Maximum allowed version (inclusive)
        
    Returns:
        True if version is compatible
    """
    try:
        v = parse_version(version)
    except ValueError:
        return False
    
    if min_version:
        min_v = parse_version(min_version)
        if v < min_v:
            return False
    
    if max_version:
        max_v = parse_version(max_version)
        if v > max_v:
            return False
    
    return True


def validate_schema_version(
    config: dict[str, Any],
    schema_type: str,
    *,
    min_version: str | None = None,
    require_version: bool = True,
) -> VersionInfo | None:
    """Validate schema version in a configuration dict.
    
    Args:
        config: Configuration dictionary
        schema_type: Type of schema (targets, license_map, etc.)
        min_version: Override minimum version (uses MIN_SUPPORTED_VERSIONS if None)
        require_version: Whether version field is required
        
    Returns:
        Parsed version info, or None if not present and not required
        
    Raises:
        MissingVersionError: If version is required but missing
        IncompatibleVersionError: If version is below minimum
    """
    version_str = config.get("schema_version")
    
    if not version_str:
        if require_version:
            raise MissingVersionError(
                f"Missing 'schema_version' field in {schema_type} configuration. "
                f"Expected version >= {min_version or MIN_SUPPORTED_VERSIONS.get(schema_type, '0.1')}"
            )
        return None
    
    version = parse_version(str(version_str))
    
    # Determine minimum version
    min_ver = min_version or MIN_SUPPORTED_VERSIONS.get(schema_type, "0.1")
    min_version_info = parse_version(min_ver)
    
    if version < min_version_info:
        raise IncompatibleVersionError(
            f"Schema version {version} for {schema_type} is too old. "
            f"Minimum required: {min_ver}. "
            f"Please update your configuration file."
        )
    
    return version


def get_current_version(schema_type: str) -> str:
    """Get the current version for a schema type.
    
    Args:
        schema_type: Type of schema
        
    Returns:
        Current version string
    """
    return CURRENT_VERSIONS.get(schema_type, "0.1")


def get_min_supported_version(schema_type: str) -> str:
    """Get the minimum supported version for a schema type.
    
    Args:
        schema_type: Type of schema
        
    Returns:
        Minimum supported version string
    """
    return MIN_SUPPORTED_VERSIONS.get(schema_type, "0.1")


def check_config_versions(
    config: dict[str, Any],
    schema_type: str,
) -> dict[str, Any]:
    """Check configuration versions and return status.
    
    Args:
        config: Configuration dictionary
        schema_type: Type of schema
        
    Returns:
        Dictionary with version check results
    """
    result: dict[str, Any] = {
        "schema_type": schema_type,
        "current_version": get_current_version(schema_type),
        "min_supported": get_min_supported_version(schema_type),
        "config_version": config.get("schema_version"),
        "is_valid": False,
        "needs_upgrade": False,
        "error": None,
    }
    
    try:
        version = validate_schema_version(config, schema_type, require_version=False)
        result["is_valid"] = True
        
        if version:
            current = parse_version(get_current_version(schema_type))
            result["needs_upgrade"] = version < current
            
    except SchemaVersionError as e:
        result["error"] = str(e)
    
    return result


__all__ = [
    "SchemaVersionError",
    "IncompatibleVersionError",
    "MissingVersionError",
    "VersionInfo",
    "parse_version",
    "is_compatible_version",
    "validate_schema_version",
    "get_current_version",
    "get_min_supported_version",
    "check_config_versions",
    "CURRENT_VERSIONS",
    "MIN_SUPPORTED_VERSIONS",
]
