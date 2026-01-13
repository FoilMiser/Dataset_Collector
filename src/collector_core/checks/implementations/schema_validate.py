"""Schema validation content check."""

from __future__ import annotations

import json
from typing import Any

from collector_core.checks.implementations.base import CheckResult

check_name = "schema_validate"


def validate_required_fields(record: dict[str, Any], required: list[str]) -> list[str]:
    """Check for required fields in record."""
    missing = []
    for field in required:
        if "." in field:
            # Nested field
            parts = field.split(".")
            value = record
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
                    break
            if value is None:
                missing.append(field)
        else:
            if field not in record or record[field] is None:
                missing.append(field)
    return missing


def validate_field_types(record: dict[str, Any], type_spec: dict[str, str]) -> list[str]:
    """Validate field types."""
    type_map = {
        "string": str,
        "str": str,
        "int": int,
        "integer": int,
        "float": float,
        "number": (int, float),
        "bool": bool,
        "boolean": bool,
        "list": list,
        "array": list,
        "dict": dict,
        "object": dict,
    }
    
    errors = []
    for field, expected_type in type_spec.items():
        if field not in record:
            continue
        
        value = record[field]
        if value is None:
            continue
        
        expected = type_map.get(expected_type.lower())
        if expected and not isinstance(value, expected):
            errors.append(f"{field}: expected {expected_type}, got {type(value).__name__}")
    
    return errors


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    """Run schema validation check.
    
    Config options:
        required_fields: List of required field paths (supports dot notation)
        field_types: Dict mapping field names to expected types
        min_text_length: Minimum length for text field
        max_text_length: Maximum length for text field
    """
    required_fields = config.get("required_fields", [])
    field_types = config.get("field_types", {})
    min_text_length = config.get("min_text_length", 0)
    max_text_length = config.get("max_text_length", float("inf"))
    
    errors: list[str] = []
    
    # Check required fields
    missing = validate_required_fields(record, required_fields)
    if missing:
        errors.extend([f"missing_field: {f}" for f in missing])
    
    # Check field types
    type_errors = validate_field_types(record, field_types)
    errors.extend(type_errors)
    
    # Check text length
    text = record.get("text", "") or ""
    text_len = len(text)
    
    if text_len < min_text_length:
        errors.append(f"text_too_short: {text_len} < {min_text_length}")
    
    if text_len > max_text_length:
        errors.append(f"text_too_long: {text_len} > {max_text_length}")
    
    details = {
        "validation_errors": errors,
        "text_length": text_len,
        "field_count": len(record),
    }
    
    if errors:
        return CheckResult(
            passed=False, action="filter",
            reason=f"schema_validation_failed: {len(errors)} errors",
            details=details, confidence=1.0,
        )
    
    return CheckResult(passed=True, action="keep", details=details, confidence=1.0)


__all__ = ["check_name", "check"]
