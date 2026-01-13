from __future__ import annotations

import re
from dataclasses import dataclass

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)


_CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.IGNORECASE)
_SHA256_RE = re.compile(r"\b[a-fA-F0-9]{64}\b")
_SHA1_RE = re.compile(r"\b[a-fA-F0-9]{40}\b")
_MD5_RE = re.compile(r"\b[a-fA-F0-9]{32}\b")

_EXPLOIT_TERMS = {"exploit", "rce", "weaponized", "shellcode", "poc", "proof-of-concept"}
_RESEARCH_TERMS = {"analysis", "research", "advisory", "report", "mitigation", "disclosure"}
_ATTACK_LABELS = {
    "phishing": "credential_access",
    "ransomware": "impact",
    "ddos": "availability",
    "sql injection": "injection",
    "xss": "injection",
    "privilege escalation": "privilege_escalation",
    "lateral movement": "lateral_movement",
}


@dataclass(frozen=True)
class _CyberSignals:
    cve_count: int
    exploit_like: bool
    research_like: bool
    malware_hashes: int
    attack_classes: list[str]
    length_score: float


def _count_matches(pattern: re.Pattern[str], text: str) -> int:
    return len(pattern.findall(text))


def _classify_attack(text: str) -> list[str]:
    lowered = text.lower()
    matches = [label for phrase, label in _ATTACK_LABELS.items() if phrase in lowered]
    return sorted(set(matches))


def _score_quality(signals: _CyberSignals) -> float:
    cve_score = 1.0 if signals.cve_count else 0.4
    context_score = 0.9 if signals.research_like else 0.5
    exploit_penalty = 0.3 if signals.exploit_like else 1.0
    hash_score = 0.7 if signals.malware_hashes else 1.0
    score = (
        0.3 * signals.length_score
        + 0.25 * cve_score
        + 0.2 * context_score
        + 0.15 * hash_score
        + 0.1 * exploit_penalty
    )
    return max(0.0, min(1.0, round(score, 3)))


def filter_record(raw: dict, ctx: DomainContext) -> FilterDecision:
    decision = standard_filter(raw, ctx)
    if not decision.allow or not decision.text:
        return decision

    text = decision.text
    cve_count = _count_matches(_CVE_RE, text)
    if not cve_count:
        return FilterDecision(
            allow=False,
            reason="cve_missing",
            text=text,
            license_spdx=decision.license_spdx,
            extra={"cve_count": 0},
            sample_extra={"cve_count": 0},
        )

    lowered = text.lower()
    exploit_like = any(term in lowered for term in _EXPLOIT_TERMS)
    research_like = any(term in lowered for term in _RESEARCH_TERMS)
    malware_hashes = sum(
        _count_matches(pattern, text) for pattern in (_SHA256_RE, _SHA1_RE, _MD5_RE)
    )
    attack_classes = _classify_attack(text)
    length_score = min(len(text) / 900, 1.0)
    signals = _CyberSignals(
        cve_count=cve_count,
        exploit_like=exploit_like,
        research_like=research_like,
        malware_hashes=malware_hashes,
        attack_classes=attack_classes,
        length_score=length_score,
    )

    if exploit_like and not research_like:
        return FilterDecision(
            allow=False,
            reason="exploit_without_context",
            text=text,
            license_spdx=decision.license_spdx,
            extra={"exploit_like": True},
            sample_extra={"exploit_like": True},
        )

    quality_score = _score_quality(signals)
    extra = {
        "quality": {
            "score": quality_score,
            "signals": {
                "cve_count": cve_count,
                "exploit_like": exploit_like,
                "research_like": research_like,
                "malware_hashes": malware_hashes,
                "attack_classes": attack_classes,
                "length_score": round(length_score, 3),
            },
        }
    }
    return FilterDecision(
        allow=True,
        text=text,
        license_spdx=decision.license_spdx,
        extra=extra,
    )


def transform_record(
    raw: dict,
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict | None:
    record = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if record is None:
        return None
    screening = {"domain": "cyber"}
    if decision.extra:
        screening.update(decision.extra)
    record["screening"] = screening
    return record


__all__ = ["filter_record", "transform_record"]
