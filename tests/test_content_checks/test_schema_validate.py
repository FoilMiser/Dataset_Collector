"""Tests for schema validation content check."""

from __future__ import annotations

import pytest
from collector_core.checks.implementations.schema_validate import (
    check, check_name,
)
from collector_core.checks.implementations.base import CheckResult


class TestSchemaCheck:
    """Tests for check function."""

    def test_valid_record_passes(self) -> None:
        """Valid record with all required fields passes."""
        record = {"text": "Some content", "id": "123", "source": "test"}
        config = {"required_fields": ["text", "id", "source"]}

        result = check(record, config)

        assert result.passed is True
        assert result.action == "keep"

    def test_missing_required_field(self) -> None:
        """Missing required field fails."""
        record = {"text": "Some content"}
        config = {"required_fields": ["text", "id"]}

        result = check(record, config)

        assert result.passed is False
        assert "missing_field: id" in result.details["validation_errors"]

    def test_nested_required_field(self) -> None:
        """Nested required fields are checked."""
        record = {"text": "Content", "source": {"url": "http://example.com"}}
        config = {"required_fields": ["source.url"]}

        result = check(record, config)

        assert result.passed is True

    def test_missing_nested_field(self) -> None:
        """Missing nested field fails."""
        record = {"text": "Content", "source": {}}
        config = {"required_fields": ["source.url"]}

        result = check(record, config)

        assert result.passed is False
        assert "missing_field: source.url" in result.details["validation_errors"]

    def test_field_type_validation(self) -> None:
        """Field types are validated."""
        record = {"text": "Content", "count": 42}
        config = {"field_types": {"text": "string", "count": "int"}}

        result = check(record, config)

        assert result.passed is True

    def test_field_type_mismatch(self) -> None:
        """Field type mismatch fails."""
        record = {"text": "Content", "count": "not a number"}
        config = {"field_types": {"count": "int"}}

        result = check(record, config)

        assert result.passed is False
        assert any("count:" in e for e in result.details["validation_errors"])

    def test_text_min_length(self) -> None:
        """Minimum text length is enforced."""
        record = {"text": "Short"}
        config = {"min_text_length": 100}

        result = check(record, config)

        assert result.passed is False
        assert any("text_too_short" in e for e in result.details["validation_errors"])

    def test_text_max_length(self) -> None:
        """Maximum text length is enforced."""
        record = {"text": "A" * 1000}
        config = {"max_text_length": 100}

        result = check(record, config)

        assert result.passed is False
        assert any("text_too_long" in e for e in result.details["validation_errors"])

    def test_text_within_bounds(self) -> None:
        """Text within bounds passes."""
        record = {"text": "A" * 50}
        config = {"min_text_length": 10, "max_text_length": 100}

        result = check(record, config)

        assert result.passed is True

    def test_null_field_type_check(self) -> None:
        """Null fields are skipped in type check."""
        record = {"text": "Content", "optional": None}
        config = {"field_types": {"optional": "string"}}

        result = check(record, config)

        assert result.passed is True

    def test_multiple_type_aliases(self) -> None:
        """Type aliases are supported."""
        record = {
            "text": "Content",
            "items": [1, 2, 3],
            "meta": {"key": "value"},
            "active": True,
        }
        config = {
            "field_types": {
                "text": "str",
                "items": "array",
                "meta": "object",
                "active": "boolean",
            }
        }

        result = check(record, config)

        assert result.passed is True

    def test_number_type(self) -> None:
        """Number type accepts int and float."""
        record = {"int_val": 42, "float_val": 3.14}
        config = {"field_types": {"int_val": "number", "float_val": "number"}}

        result = check(record, config)

        assert result.passed is True

    def test_details_include_text_length(self) -> None:
        """Details include text length."""
        record = {"text": "Hello world"}
        config = {}

        result = check(record, config)

        assert "text_length" in result.details
        assert result.details["text_length"] == 11

    def test_details_include_field_count(self) -> None:
        """Details include field count."""
        record = {"a": 1, "b": 2, "c": 3}
        config = {}

        result = check(record, config)

        assert result.details["field_count"] == 3


class TestCheckName:
    """Test check name constant."""

    def test_check_name(self) -> None:
        """Check name is set correctly."""
        assert check_name == "schema_validate"
