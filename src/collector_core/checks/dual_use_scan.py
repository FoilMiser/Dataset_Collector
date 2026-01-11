from __future__ import annotations

import re
from typing import Any

from collector_core.checks.base import BaseCheck, CheckContext
from collector_core.checks.registry import register_check
from collector_core.checks.utils import build_text_blob, resolve_check_action

KEYWORD_PATTERNS = {
    "explosives": re.compile(r"\bexplosive(s)?\b", re.IGNORECASE),
    "weapon": re.compile(r"\bweapon(s|ize|ized)?\b", re.IGNORECASE),
    "bioweapon": re.compile(r"\bbioweapon(s)?\b", re.IGNORECASE),
    "pathogen": re.compile(r"\bpathogen(s|ic)?\b", re.IGNORECASE),
    "anthrax": re.compile(r"\banthrax\b", re.IGNORECASE),
    "nerve_agent": re.compile(r"\bnerve agent(s)?\b", re.IGNORECASE),
    "toxin": re.compile(r"\btoxin(s)?\b", re.IGNORECASE),
    "malware": re.compile(r"\bmalware\b", re.IGNORECASE),
    "ransomware": re.compile(r"\bransomware\b", re.IGNORECASE),
    "exploit": re.compile(r"\bexploit(s|ing)?\b", re.IGNORECASE),
    "payload": re.compile(r"\b(payload|shellcode)\b", re.IGNORECASE),
    "improvised_explosive_device": re.compile(
        r"\bimprovised explosive device(s)?\b", re.IGNORECASE
    ),
    "weaponized": re.compile(r"\bweaponized\b", re.IGNORECASE),
}


@register_check
class DualUseScanCheck(BaseCheck):
    name = "dual_use_scan"
    description = "Keyword scan for potential dual-use content."

    def run(self, ctx: CheckContext) -> dict[str, Any]:
        text = build_text_blob(ctx)
        hits: dict[str, int] = {}
        match_count = 0
        for label, pattern in KEYWORD_PATTERNS.items():
            found = pattern.findall(text)
            if found:
                hits[label] = len(found)
                match_count += len(found)

        if match_count == 0:
            return {"status": "ok", "match_count": 0}

        action = resolve_check_action(ctx, self.check_name()) or "warn"
        status = action if action in {"block", "quarantine"} else "warn"
        return {
            "status": status,
            "action": action,
            "match_count": match_count,
            "keyword_hits": hits,
        }
