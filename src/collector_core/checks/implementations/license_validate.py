from __future__ import annotations

from dataclasses import dataclass
from typing import Any

check_name = "license_validate"


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    details: dict[str, Any]


def _normalize_allowed(config: dict[str, Any]) -> tuple[list[str], str]:
    allowed = config.get("allowed_licenses") or []
    if isinstance(allowed, str):
        allowed = [allowed]
    if not isinstance(allowed, list):
        allowed = []
    allowed_list = [str(lic).strip() for lic in allowed if str(lic).strip()]
    field = str(config.get("license_field") or "license")
    return allowed_list, field


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    config = config or {}
    record = record or {}
    allowed, field = _normalize_allowed(config)
    value = record.get(field)
    if value is None:
        return CheckResult(
            check=check_name,
            status="warn",
            details={"reason": "missing_license", "field": field},
        )
    if isinstance(value, list):
        licenses = [str(item).strip() for item in value if str(item).strip()]
    else:
        licenses = [str(value).strip()]
    if not allowed:
        return CheckResult(
            check=check_name,
            status="ok",
            details={"licenses": licenses, "reason": "allowed_licenses_not_configured"},
        )
    allowed_set = {lic.lower() for lic in allowed}
    status = "ok" if any(lic.lower() in allowed_set for lic in licenses) else "fail"
    return CheckResult(
        check=check_name,
        status=status,
        details={"licenses": licenses, "allowed_licenses": allowed},
    )
