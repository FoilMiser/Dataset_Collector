"""Cybersecurity-specific yellow screening.

This module provides cybersecurity-specific filtering including:
- CVE ID validation
- Exploit code detection (differentiate from security research)
- Malware hash detection
- Attack pattern classification
"""

from __future__ import annotations

import re
from typing import Any

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)

# CVE ID pattern
CVE_PATTERN = re.compile(r"\bCVE-(\d{4})-(\d{4,7})\b", re.IGNORECASE)

# MITRE ATT&CK patterns
ATTACK_PATTERN = re.compile(r"\b(T\d{4}(?:\.\d{3})?)\b")

# CWE pattern
CWE_PATTERN = re.compile(r"\bCWE-(\d{1,4})\b", re.IGNORECASE)

# Malware hash patterns
HASH_PATTERNS = {
    "md5": re.compile(r"\b([a-fA-F0-9]{32})\b"),
    "sha1": re.compile(r"\b([a-fA-F0-9]{40})\b"),
    "sha256": re.compile(r"\b([a-fA-F0-9]{64})\b"),
}

# Exploit/weaponization patterns
EXPLOIT_PATTERNS = [
    re.compile(r"\b(exploit|payload|shellcode|rop\s+chain|buffer\s+overflow)\b", re.IGNORECASE),
    re.compile(r"\b(0day|zero[- ]day|n-day)\s+(exploit|vulnerability)\b", re.IGNORECASE),
    re.compile(r"\b(remote\s+code\s+execution|rce|arbitrary\s+code)\b", re.IGNORECASE),
]

# Threat intelligence indicators
THREAT_INTEL_INDICATORS = [
    "indicator of compromise", "ioc", "threat actor", "apt",
    "campaign", "malware family", "c2", "command and control",
    "phishing", "ransomware", "backdoor", "trojan",
]

# Security research indicators (legitimate)
RESEARCH_INDICATORS = [
    "responsible disclosure", "coordinated disclosure", "security research",
    "proof of concept", "poc", "vulnerability assessment", "penetration test",
    "bug bounty", "cvss", "severity", "mitigation", "patch",
]


def extract_cve_ids(text: str) -> list[dict[str, Any]]:
    """Extract and validate CVE IDs from text."""
    results = []
    seen: set[str] = set()
    for match in CVE_PATTERN.finditer(text):
        cve_id = match.group(0).upper()
        if cve_id not in seen:
            seen.add(cve_id)
            year = int(match.group(1))
            results.append({
                "cve_id": cve_id,
                "year": year,
                "is_valid_format": 1999 <= year <= 2030,
            })
    return results


def extract_attack_techniques(text: str) -> list[str]:
    """Extract MITRE ATT&CK technique IDs."""
    matches = ATTACK_PATTERN.findall(text)
    return list(set(matches))[:20]


def extract_cwe_ids(text: str) -> list[str]:
    """Extract CWE IDs."""
    matches = CWE_PATTERN.findall(text)
    return [f"CWE-{m}" for m in set(matches)][:20]


def detect_hashes(text: str) -> dict[str, int]:
    """Detect potential malware hashes."""
    results = {}
    for hash_type, pattern in HASH_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            results[hash_type] = len(matches)
    return results


def detect_exploit_content(text: str) -> tuple[bool, list[str]]:
    """Detect potential exploit code or descriptions."""
    matches = []
    for pattern in EXPLOIT_PATTERNS:
        found = pattern.findall(text)
        if found:
            matches.extend(found[:3])
    return len(matches) > 0, matches


def check_threat_intel(text: str) -> tuple[int, list[str]]:
    """Check for threat intelligence indicators."""
    text_lower = text.lower()
    matched = [ind for ind in THREAT_INTEL_INDICATORS if ind in text_lower]
    return len(matched), matched


def check_research_indicators(text: str) -> tuple[int, list[str]]:
    """Check for legitimate security research indicators."""
    text_lower = text.lower()
    matched = [ind for ind in RESEARCH_INDICATORS if ind in text_lower]
    return len(matched), matched


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Cybersecurity-specific filtering."""
    text = raw.get("text", "") or raw.get("content", "") or raw.get("description", "") or ""

    # Extract security identifiers
    cve_ids = extract_cve_ids(text)
    attack_techniques = extract_attack_techniques(text)
    cwe_ids = extract_cwe_ids(text)
    hash_counts = detect_hashes(text)

    # Check for exploit content
    has_exploit, exploit_matches = detect_exploit_content(text)

    # Check threat intelligence and research indicators
    threat_score, threat_matches = check_threat_intel(text)
    research_score, research_matches = check_research_indicators(text)

    # Run standard filter
    decision = standard_filter(raw, ctx)

    # Add cyber-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update({
        "cve_ids": cve_ids,
        "cve_count": len(cve_ids),
        "attack_techniques": attack_techniques,
        "cwe_ids": cwe_ids,
        "hash_counts": hash_counts,
        "has_exploit_content": has_exploit,
        "exploit_indicators": exploit_matches,
        "threat_intel_score": threat_score,
        "threat_indicators": threat_matches,
        "research_score": research_score,
        "research_indicators": research_matches,
    })

    # Flag potential exploit content but check for research context
    if has_exploit and research_score < 2:
        decision.extra["potential_weaponized"] = True
    elif has_exploit and research_score >= 2:
        decision.extra["security_research"] = True

    return decision


def transform_record(
    raw: dict[str, Any], ctx: DomainContext, decision: FilterDecision, *, license_profile: str,
) -> dict[str, Any] | None:
    """Transform cybersecurity record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if result is None:
        return None
    extra = decision.extra or {}
    if extra.get("cve_ids"):
        result["cve_ids"] = [c["cve_id"] for c in extra["cve_ids"]]
    if extra.get("attack_techniques"):
        result["attack_techniques"] = extra["attack_techniques"]
    if extra.get("cwe_ids"):
        result["cwe_ids"] = extra["cwe_ids"]
    if extra.get("potential_weaponized"):
        result["_potential_weaponized"] = True
    if extra.get("security_research"):
        result["is_security_research"] = True
    return result


__all__ = ["filter_record", "transform_record", "extract_cve_ids"]
