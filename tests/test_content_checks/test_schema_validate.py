from __future__ import annotations

from collector_core.checks.implementations import schema_validate


def test_schema_validate_reports_missing_fields() -> None:
    result = schema_validate.check({"id": "1"}, {"required_fields": ["id", "text"]})
    assert result.check == schema_validate.check_name
    assert result.status == "fail"
    assert result.details["missing_fields"] == ["text"]


def test_schema_validate_skips_without_configuration() -> None:
    result = schema_validate.check({"id": "1"}, {})
    assert result.status == "skip"
    assert result.details["reason"] == "required_fields_not_configured"
