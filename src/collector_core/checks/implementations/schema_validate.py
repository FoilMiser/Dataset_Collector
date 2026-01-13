from __future__ import annotations

from dataclasses import dataclass
from typing import Any

check_name = "schema_validate"


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    details: dict[str, Any]


def _normalize_required_fields(config: dict[str, Any]) -> list[str]:
    required = config.get("required_fields") or []
    if isinstance(required, str):
        required = [required]
    if not isinstance(required, list):
        required = []
    return [str(field).strip() for field in required if str(field).strip()]


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    config = config or {}
    record = record or {}
    required_fields = _normalize_required_fields(config)
    if not required_fields:
        return CheckResult(
            check=check_name,
            status="skip",
            details={"reason": "required_fields_not_configured"},
        )
    missing = [field for field in required_fields if record.get(field) is None]
    status = "ok" if not missing else "fail"
    return CheckResult(
        check=check_name,
        status=status,
        details={"required_fields": required_fields, "missing_fields": missing},
    )
