from __future__ import annotations

import re
from typing import Any

from collector_core.checks.base import BaseCheck, CheckContext
from collector_core.checks.registry import register_check
from collector_core.checks.utils import build_text_blob, resolve_check_action

PATTERNS: dict[str, re.Pattern[str]] = {
    "aws_access_key_id": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "aws_secret_access_key": re.compile(
        r"\b(?=[A-Za-z0-9/+=]{40}\b)(?=.*[A-Z])(?=.*[a-z])(?=.*\d)[A-Za-z0-9/+=]{40}\b"
    ),
    "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"),
    "slack_token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    "google_api_key": re.compile(r"\bAIza[0-9A-Za-z\-_]{35}\b"),
    "stripe_key": re.compile(r"\bsk_live_[0-9a-zA-Z]{24}\b"),
    "twilio_key": re.compile(r"\bSK[0-9a-fA-F]{32}\b"),
}


def _mask_value(value: str, keep: int = 4) -> str:
    if len(value) <= keep:
        return "*" * len(value)
    return f"{value[:keep]}***{value[-keep:]}"


@register_check
class SecretScanCheck(BaseCheck):
    name = "secret_scan"
    description = "Heuristic scan for common API key formats."

    def run(self, ctx: CheckContext) -> dict[str, Any]:
        text = build_text_blob(ctx)
        matches: dict[str, list[str]] = {}
        match_count = 0
        for label, pattern in PATTERNS.items():
            found = pattern.findall(text)
            if found:
                matches[label] = [_mask_value(value) for value in found[:5]]
                match_count += len(found)

        if match_count == 0:
            return {"status": "ok", "match_count": 0}

        action = resolve_check_action(ctx, self.check_name()) or "warn"
        status = action if action in {"block", "quarantine"} else "warn"
        return {
            "status": status,
            "action": action,
            "match_count": match_count,
            "matches": matches,
        }
