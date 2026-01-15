"""Tests for config_validator module."""

from __future__ import annotations

from pathlib import Path

import pytest

from collector_core.config_validator import (
    _expand_includes,
    load_schema,
    read_yaml,
    validate_config,
)
from collector_core.exceptions import ConfigValidationError, YamlParseError


class TestLoadSchema:
    """Tests for schema loading."""

    def test_load_schema_success(self) -> None:
        """load_schema should successfully load a known schema."""
        schema = load_schema("targets")
        assert isinstance(schema, dict)
        assert "$schema" in schema or "type" in schema

    def test_load_schema_missing(self) -> None:
        """load_schema should raise FileNotFoundError for missing schema."""
        with pytest.raises(FileNotFoundError):
            load_schema("nonexistent_schema_xyz123")

    def test_load_schema_caching(self) -> None:
        """load_schema should cache results."""
        schema1 = load_schema("targets")
        schema2 = load_schema("targets")
        # Should return the same cached object
        assert schema1 is schema2


class TestValidateConfig:
    """Tests for config validation."""

    def test_validate_config_valid(self, tmp_path: Path) -> None:
        """validate_config should not raise for valid config."""
        # Minimal valid targets config (using schema_version 0.9)
        config = {
            "schema_version": "0.9",
            "targets": [],
        }
        # Should not raise
        validate_config(config, "targets")

    def test_validate_config_invalid_type(self, tmp_path: Path) -> None:
        """validate_config should raise ConfigValidationError for invalid type."""
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            pytest.skip("jsonschema not installed")

        config = {
            "schema_version": "2.0",
            "targets": "not_a_list",  # Should be a list
        }
        with pytest.raises(ConfigValidationError) as exc_info:
            validate_config(config, "targets")
        assert "targets" in str(exc_info.value).lower()

    def test_validate_config_missing_required_field(self, tmp_path: Path) -> None:
        """validate_config should raise for missing required fields."""
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            pytest.skip("jsonschema not installed")

        config = {
            # Missing schema_version
            "targets": [],
        }
        with pytest.raises(ConfigValidationError):
            validate_config(config, "targets")


class TestReadYaml:
    """Tests for YAML reading."""

    def test_read_yaml_basic(self, tmp_path: Path) -> None:
        """read_yaml should parse basic YAML."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("key: value\nnumber: 42\n")

        result = read_yaml(yaml_file)
        assert result == {"key": "value", "number": 42}

    def test_read_yaml_empty_file(self, tmp_path: Path) -> None:
        """read_yaml should return empty dict for empty file."""
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("")

        result = read_yaml(yaml_file)
        assert result == {}

    def test_read_yaml_invalid_yaml(self, tmp_path: Path) -> None:
        """read_yaml should raise YamlParseError for invalid YAML."""
        yaml_file = tmp_path / "invalid.yaml"
        yaml_file.write_text("key: value\n  bad indentation\n: invalid")

        with pytest.raises(YamlParseError) as exc_info:
            read_yaml(yaml_file)
        assert str(yaml_file) in str(exc_info.value)

    def test_read_yaml_with_schema_validation(self, tmp_path: Path) -> None:
        """read_yaml should validate against schema if provided."""
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            pytest.skip("jsonschema not installed")

        yaml_file = tmp_path / "config.yaml"
        # Invalid targets config (targets should be a list)
        yaml_file.write_text("schema_version: '2.0'\ntargets: invalid_value\n")

        with pytest.raises(ConfigValidationError):
            read_yaml(yaml_file, schema_name="targets")


class TestExpandIncludes:
    """Tests for YAML include expansion."""

    def test_expand_includes_no_includes(self, tmp_path: Path) -> None:
        """_expand_includes should return unchanged text with no includes."""
        text = "key: value\nother: data\n"
        result = _expand_includes(text, tmp_path)
        assert result == text

    def test_expand_includes_basic(self, tmp_path: Path) -> None:
        """_expand_includes should expand basic includes."""
        # Create included file
        included = tmp_path / "included.yaml"
        included.write_text("nested: value\ndata: 123\n")

        # Main file with include
        text = "main: data\nconfig: !include included.yaml\n"
        result = _expand_includes(text, tmp_path)

        assert "main: data" in result
        assert "config:" in result
        assert "nested: value" in result

    def test_expand_includes_preserves_indentation(self, tmp_path: Path) -> None:
        """_expand_includes should preserve indentation."""
        included = tmp_path / "included.yaml"
        included.write_text("key: value\n")

        text = "  config: !include included.yaml\n"
        result = _expand_includes(text, tmp_path)

        lines = result.splitlines()
        assert any(line.startswith("    ") for line in lines)  # Included content indented

    def test_expand_includes_path_traversal_blocked(self, tmp_path: Path) -> None:
        """_expand_includes should block path traversal outside repository."""
        # Try to include a file outside repo (../../etc/passwd)
        text = "config: !include ../../etc/passwd\n"

        with pytest.raises(ValueError) as exc_info:
            _expand_includes(text, tmp_path, repo_root=tmp_path)
        assert "escapes repository" in str(exc_info.value)

    def test_expand_includes_absolute_path_blocked(self, tmp_path: Path) -> None:
        """_expand_includes should block absolute paths outside repository."""
        text = "config: !include /etc/passwd\n"

        with pytest.raises(ValueError) as exc_info:
            _expand_includes(text, tmp_path, repo_root=tmp_path)
        assert "escapes repository" in str(exc_info.value)

    def test_expand_includes_cross_directory_within_repo(self, tmp_path: Path) -> None:
        """_expand_includes should allow cross-directory includes within repo."""
        # Create directory structure: repo/subdir1/file.yaml includes repo/subdir2/shared.yaml
        subdir1 = tmp_path / "subdir1"
        subdir2 = tmp_path / "subdir2"
        subdir1.mkdir()
        subdir2.mkdir()

        shared = subdir2 / "shared.yaml"
        shared.write_text("shared_data: from_subdir2\n")

        # File in subdir1 includes ../subdir2/shared.yaml
        text = "local: data\nconfig: !include ../subdir2/shared.yaml\n"
        result = _expand_includes(text, subdir1, repo_root=tmp_path)

        assert "local: data" in result
        assert "shared_data: from_subdir2" in result

    def test_expand_includes_symlink_blocked(self, tmp_path: Path) -> None:
        """_expand_includes should block symlinks."""
        # Create a regular file
        target = tmp_path / "target.yaml"
        target.write_text("data: value\n")

        # Create a symlink to it
        link = tmp_path / "link.yaml"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("Symlinks not supported on this system")

        # Verify the symlink was created
        if not link.is_symlink():
            pytest.skip("Symlink creation succeeded but is_symlink() returns False")

        # Try to include via symlink
        text = "config: !include link.yaml\n"

        with pytest.raises(ValueError) as exc_info:
            _expand_includes(text, tmp_path, repo_root=tmp_path)
        assert "Symlinks not allowed" in str(exc_info.value)

    def test_expand_includes_nested(self, tmp_path: Path) -> None:
        """_expand_includes should handle nested includes."""
        # Create nested include chain
        innermost = tmp_path / "innermost.yaml"
        innermost.write_text("innermost: data\n")

        middle = tmp_path / "middle.yaml"
        middle.write_text("middle: data\ninner: !include innermost.yaml\n")

        # Main file
        text = "main: data\nconfig: !include middle.yaml\n"
        result = _expand_includes(text, tmp_path)

        assert "main: data" in result
        assert "middle: data" in result
        assert "innermost: data" in result

    def test_expand_includes_missing_file(self, tmp_path: Path) -> None:
        """_expand_includes should raise FileNotFoundError for missing include."""
        text = "config: !include nonexistent.yaml\n"

        with pytest.raises(FileNotFoundError):
            _expand_includes(text, tmp_path)

    def test_expand_includes_preserves_trailing_newline(self, tmp_path: Path) -> None:
        """_expand_includes should preserve trailing newline."""
        text = "key: value\n"
        result = _expand_includes(text, tmp_path)
        assert result.endswith("\n")

        text_no_newline = "key: value"
        result_no_newline = _expand_includes(text_no_newline, tmp_path)
        assert not result_no_newline.endswith("\n")

    def test_expand_includes_with_comments(self, tmp_path: Path) -> None:
        """_expand_includes should handle YAML comments in include lines."""
        included = tmp_path / "included.yaml"
        included.write_text("data: value\n")

        # Include with comment after path
        text = "config: !include included.yaml  # This is a comment\n"
        result = _expand_includes(text, tmp_path)

        assert "data: value" in result
        assert "#" not in result  # Comment should be stripped

    def test_expand_includes_quoted_paths(self, tmp_path: Path) -> None:
        """_expand_includes should handle quoted paths."""
        included = tmp_path / "file with spaces.yaml"
        included.write_text("data: value\n")

        # Test single quotes
        text = "config: !include 'file with spaces.yaml'\n"
        result = _expand_includes(text, tmp_path)
        assert "data: value" in result

        # Test double quotes
        text2 = 'config: !include "file with spaces.yaml"\n'
        result2 = _expand_includes(text2, tmp_path)
        assert "data: value" in result2
