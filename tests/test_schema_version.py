"""
Tests for schema version validation and enforcement.

Issue 4.3 (v3.0): Tests for schema version checking.
"""

from __future__ import annotations

import pytest
from collector_core.schema_version import (
    VersionInfo,
    parse_version,
    is_compatible_version,
    validate_schema_version,
    get_current_version,
    get_min_supported_version,
    check_config_versions,
    SchemaVersionError,
    IncompatibleVersionError,
    MissingVersionError,
    CURRENT_VERSIONS,
    MIN_SUPPORTED_VERSIONS,
)


class TestVersionInfo:
    """Tests for VersionInfo dataclass."""

    def test_creation(self) -> None:
        """VersionInfo can be created."""
        v = VersionInfo(major=1, minor=2, patch=3, raw="1.2.3")
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3
        assert v.raw == "1.2.3"

    def test_str(self) -> None:
        """String representation works."""
        v = VersionInfo(major=1, minor=2, patch=3, raw="1.2.3")
        assert str(v) == "1.2.3"

    def test_str_without_raw(self) -> None:
        """String representation without raw."""
        v = VersionInfo(major=1, minor=2, patch=3)
        assert str(v) == "1.2.3"

    def test_comparison_lt(self) -> None:
        """Less than comparison."""
        v1 = VersionInfo(major=1, minor=0, patch=0)
        v2 = VersionInfo(major=2, minor=0, patch=0)
        assert v1 < v2
        assert not v2 < v1

    def test_comparison_le(self) -> None:
        """Less than or equal comparison."""
        v1 = VersionInfo(major=1, minor=0, patch=0)
        v2 = VersionInfo(major=1, minor=0, patch=0)
        v3 = VersionInfo(major=2, minor=0, patch=0)
        assert v1 <= v2
        assert v1 <= v3
        assert not v3 <= v1

    def test_comparison_gt(self) -> None:
        """Greater than comparison."""
        v1 = VersionInfo(major=2, minor=0, patch=0)
        v2 = VersionInfo(major=1, minor=0, patch=0)
        assert v1 > v2
        assert not v2 > v1

    def test_comparison_ge(self) -> None:
        """Greater than or equal comparison."""
        v1 = VersionInfo(major=2, minor=0, patch=0)
        v2 = VersionInfo(major=2, minor=0, patch=0)
        v3 = VersionInfo(major=1, minor=0, patch=0)
        assert v1 >= v2
        assert v1 >= v3
        assert not v3 >= v1

    def test_comparison_eq(self) -> None:
        """Equality comparison."""
        v1 = VersionInfo(major=1, minor=2, patch=3)
        v2 = VersionInfo(major=1, minor=2, patch=3)
        v3 = VersionInfo(major=1, minor=2, patch=4)
        assert v1 == v2
        assert not v1 == v3

    def test_comparison_with_non_version(self) -> None:
        """Comparison with non-VersionInfo returns NotImplemented."""
        v = VersionInfo(major=1, minor=0, patch=0)
        assert v.__eq__("1.0.0") is NotImplemented


class TestParseVersion:
    """Tests for parse_version function."""

    def test_simple_version(self) -> None:
        """Parse simple version."""
        v = parse_version("1.0")
        assert v.major == 1
        assert v.minor == 0
        assert v.patch == 0

    def test_full_version(self) -> None:
        """Parse full version with patch."""
        v = parse_version("1.2.3")
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_v_prefix(self) -> None:
        """Parse version with v prefix."""
        v = parse_version("v1.2.3")
        assert v.major == 1
        assert v.minor == 2

    def test_suffix(self) -> None:
        """Parse version with suffix (beta, rc, etc)."""
        v = parse_version("1.2.3-beta")
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_empty_version(self) -> None:
        """Empty version raises ValueError."""
        with pytest.raises(ValueError, match="Empty"):
            parse_version("")

    def test_invalid_format(self) -> None:
        """Invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid"):
            parse_version("abc")

    def test_single_number(self) -> None:
        """Single number raises ValueError."""
        with pytest.raises(ValueError, match="Invalid"):
            parse_version("1")


class TestIsCompatibleVersion:
    """Tests for is_compatible_version function."""

    def test_within_range(self) -> None:
        """Version within range is compatible."""
        result = is_compatible_version("1.5", min_version="1.0", max_version="2.0")
        assert result is True

    def test_at_min(self) -> None:
        """Version at minimum is compatible."""
        result = is_compatible_version("1.0", min_version="1.0")
        assert result is True

    def test_at_max(self) -> None:
        """Version at maximum is compatible."""
        result = is_compatible_version("2.0", max_version="2.0")
        assert result is True

    def test_below_min(self) -> None:
        """Version below minimum is incompatible."""
        result = is_compatible_version("0.5", min_version="1.0")
        assert result is False

    def test_above_max(self) -> None:
        """Version above maximum is incompatible."""
        result = is_compatible_version("3.0", max_version="2.0")
        assert result is False

    def test_invalid_version(self) -> None:
        """Invalid version is incompatible."""
        result = is_compatible_version("invalid", min_version="1.0")
        assert result is False

    def test_no_bounds(self) -> None:
        """No bounds means always compatible."""
        result = is_compatible_version("99.99")
        assert result is True


class TestValidateSchemaVersion:
    """Tests for validate_schema_version function."""

    def test_valid_version(self) -> None:
        """Valid version passes."""
        config = {"schema_version": "0.9"}
        version = validate_schema_version(config, "targets")
        assert version is not None
        assert version.major == 0
        assert version.minor == 9

    def test_missing_required(self) -> None:
        """Missing required version raises."""
        config = {}
        with pytest.raises(MissingVersionError):
            validate_schema_version(config, "targets", require_version=True)

    def test_missing_optional(self) -> None:
        """Missing optional version returns None."""
        config = {}
        version = validate_schema_version(config, "targets", require_version=False)
        assert version is None

    def test_incompatible_version(self) -> None:
        """Incompatible version raises."""
        config = {"schema_version": "0.1"}
        with pytest.raises(IncompatibleVersionError):
            validate_schema_version(config, "targets", min_version="0.8")

    def test_custom_min_version(self) -> None:
        """Custom min_version is used."""
        config = {"schema_version": "0.5"}
        with pytest.raises(IncompatibleVersionError):
            validate_schema_version(config, "targets", min_version="0.6")


class TestGetCurrentVersion:
    """Tests for get_current_version function."""

    def test_known_schema(self) -> None:
        """Known schema type returns version."""
        version = get_current_version("targets")
        assert version == CURRENT_VERSIONS["targets"]

    def test_unknown_schema(self) -> None:
        """Unknown schema type returns default."""
        version = get_current_version("unknown")
        assert version == "0.1"


class TestGetMinSupportedVersion:
    """Tests for get_min_supported_version function."""

    def test_known_schema(self) -> None:
        """Known schema type returns min version."""
        version = get_min_supported_version("targets")
        assert version == MIN_SUPPORTED_VERSIONS["targets"]

    def test_unknown_schema(self) -> None:
        """Unknown schema type returns default."""
        version = get_min_supported_version("unknown")
        assert version == "0.1"


class TestCheckConfigVersions:
    """Tests for check_config_versions function."""

    def test_valid_config(self) -> None:
        """Valid config returns positive result."""
        config = {"schema_version": "0.9"}
        result = check_config_versions(config, "targets")

        assert result["is_valid"] is True
        assert result["error"] is None
        assert result["config_version"] == "0.9"

    def test_missing_version(self) -> None:
        """Missing version in result."""
        config = {}
        result = check_config_versions(config, "targets")

        # Still valid since require_version=False in check
        assert result["config_version"] is None

    def test_needs_upgrade(self) -> None:
        """Detects when upgrade is needed."""
        config = {"schema_version": "0.8"}
        result = check_config_versions(config, "targets")

        assert result["is_valid"] is True
        assert result["needs_upgrade"] is True

    def test_current_version(self) -> None:
        """Current version doesn't need upgrade."""
        current = get_current_version("targets")
        config = {"schema_version": current}
        result = check_config_versions(config, "targets")

        assert result["needs_upgrade"] is False


class TestExceptions:
    """Tests for exception classes."""

    def test_schema_version_error(self) -> None:
        """SchemaVersionError is base exception."""
        with pytest.raises(SchemaVersionError):
            raise SchemaVersionError("test")

    def test_incompatible_version_error(self) -> None:
        """IncompatibleVersionError inherits from base."""
        with pytest.raises(SchemaVersionError):
            raise IncompatibleVersionError("test")

    def test_missing_version_error(self) -> None:
        """MissingVersionError inherits from base."""
        with pytest.raises(SchemaVersionError):
            raise MissingVersionError("test")


class TestEdgeCases:
    """Edge case tests."""

    def test_version_with_whitespace(self) -> None:
        """Version with whitespace is handled."""
        v = parse_version("  1.0.0  ")
        assert v.major == 1

    def test_numeric_version_in_config(self) -> None:
        """Numeric version in config is converted."""
        config = {"schema_version": 0.9}  # Number, not string
        version = validate_schema_version(config, "targets")
        assert version is not None

    def test_all_schema_types_have_versions(self) -> None:
        """All schema types have current and min versions."""
        for schema_type in CURRENT_VERSIONS:
            assert schema_type in MIN_SUPPORTED_VERSIONS
            # Min should not exceed current
            min_v = parse_version(MIN_SUPPORTED_VERSIONS[schema_type])
            cur_v = parse_version(CURRENT_VERSIONS[schema_type])
            assert min_v <= cur_v
