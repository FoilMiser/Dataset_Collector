from __future__ import annotations

from dataclasses import dataclass
from typing import Any

check_name = "language_detect"


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    details: dict[str, Any]


def _normalize_allowed(config: dict[str, Any]) -> tuple[list[str], str]:
    allowed = config.get("allowed_languages") or []
    if isinstance(allowed, str):
        allowed = [allowed]
    if not isinstance(allowed, list):
        allowed = []
    allowed_list = [str(lang).strip().lower() for lang in allowed if str(lang).strip()]
    field = str(config.get("language_field") or "language")
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
            details={"reason": "missing_language", "field": field},
        )
    if isinstance(value, list):
        languages = [str(item).strip().lower() for item in value if str(item).strip()]
    else:
        languages = [str(value).strip().lower()]
    if not allowed:
        return CheckResult(
            check=check_name,
            status="ok",
            details={"languages": languages, "reason": "allowed_languages_not_configured"},
        )
    allowed_set = set(allowed)
    status = "ok" if any(lang in allowed_set for lang in languages) else "fail"
    return CheckResult(
        check=check_name,
        status=status,
        details={"languages": languages, "allowed_languages": allowed},
    )
