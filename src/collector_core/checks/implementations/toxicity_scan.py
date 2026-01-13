from __future__ import annotations

from dataclasses import dataclass
from typing import Any

check_name = "toxicity_scan"


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    details: dict[str, Any]


def _normalize_terms(config: dict[str, Any]) -> tuple[list[str], str]:
    terms = config.get("toxicity_terms") or []
    if isinstance(terms, str):
        terms = [terms]
    if not isinstance(terms, list):
        terms = []
    term_list = [str(term).strip().lower() for term in terms if str(term).strip()]
    field = str(config.get("text_field") or "text")
    return term_list, field


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    config = config or {}
    record = record or {}
    terms, field = _normalize_terms(config)
    if not terms:
        return CheckResult(
            check=check_name,
            status="skip",
            details={"reason": "toxicity_terms_not_configured"},
        )
    text = record.get(field)
    if not text:
        return CheckResult(
            check=check_name,
            status="warn",
            details={"reason": "missing_text", "field": field},
        )
    normalized = str(text).lower()
    match_counts: dict[str, int] = {}
    for term in terms:
        count = normalized.count(term)
        if count:
            match_counts[term] = count
    match_total = sum(match_counts.values())
    status = "warn" if match_total else "ok"
    return CheckResult(
        check=check_name,
        status=status,
        details={"match_count": match_total, "matches": match_counts},
    )
