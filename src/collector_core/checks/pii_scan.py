from __future__ import annotations

import re
from typing import Any

from collector_core.checks.base import BaseCheck, CheckContext
from collector_core.checks.registry import register_check
from collector_core.checks.utils import build_text_blob, resolve_check_action

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(
    r"\b(?:\+?1[\s.-]?)?(?:\(\s*\d{3}\s*\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}\b"
)
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")


def _mask_value(value: str, keep: int = 2) -> str:
    if len(value) <= keep:
        return "*" * len(value)
    return f"{value[:keep]}***{value[-keep:]}"


def _summarize_matches(matches: list[str], limit: int = 5) -> list[str]:
    return [_mask_value(m) for m in matches[:limit]]


@register_check
class PiiScanCheck(BaseCheck):
    name = "pii_scan"
    description = "Heuristic scan for PII-like patterns."

    def run(self, ctx: CheckContext) -> dict[str, Any]:
        text = build_text_blob(ctx)
        emails = EMAIL_RE.findall(text)
        phones = PHONE_RE.findall(text)
        ssns = SSN_RE.findall(text)
        match_count = len(emails) + len(phones) + len(ssns)
        if match_count == 0:
            return {"status": "ok", "match_count": 0}

        action = resolve_check_action(ctx, self.check_name()) or "warn"
        status = action if action in {"block", "quarantine"} else "warn"
        return {
            "status": status,
            "action": action,
            "match_count": match_count,
            "emails": _summarize_matches(emails),
            "phones": _summarize_matches(phones),
            "ssns": _summarize_matches(ssns),
        }
