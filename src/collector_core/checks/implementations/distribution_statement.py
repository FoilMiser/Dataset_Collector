from __future__ import annotations

from dataclasses import dataclass
from typing import Any

check_name = "distribution_statement"


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    details: dict[str, Any]


def _normalize_allowed(config: dict[str, Any]) -> tuple[list[str], str]:
    allowed = config.get("allowed_statements") or []
    if isinstance(allowed, str):
        allowed = [allowed]
    if not isinstance(allowed, list):
        allowed = []
    allowed_list = [str(statement).strip() for statement in allowed if str(statement).strip()]
    field = str(config.get("statement_field") or "distribution_statement")
    return allowed_list, field


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    config = config or {}
    record = record or {}
    allowed, field = _normalize_allowed(config)
    statement = record.get(field)
    if not statement:
        return CheckResult(
            check=check_name,
            status="warn",
            details={"reason": "missing_statement", "field": field},
        )
    statement_text = str(statement).strip()
    if not allowed:
        return CheckResult(
            check=check_name,
            status="ok",
            details={"statement": statement_text, "reason": "allowed_statements_not_configured"},
        )
    status = "ok" if statement_text in allowed else "fail"
    return CheckResult(
        check=check_name,
        status=status,
        details={"statement": statement_text, "allowed_statements": allowed},
    )
