"""Toxicity and harmful content detection check."""

from __future__ import annotations

import re
from typing import Any

from collector_core.checks.implementations.base import CheckResult

check_name = "toxicity_scan"

# Hate speech / violence patterns
HATE_PATTERNS = [
    re.compile(r"\b(kill|murder|attack)\s+(all|every|the)\s+\w+s?\b", re.IGNORECASE),
    re.compile(r"\b(hate|destroy|eliminate)\s+(all|every)\s+\w+\b", re.IGNORECASE),
    re.compile(r"\bdeath\s+to\s+\w+\b", re.IGNORECASE),
    re.compile(r"\b(inferior|subhuman)\s+(race|people|group)\b", re.IGNORECASE),
    re.compile(r"\b(genocide|ethnic\s+cleansing|exterminate)\b", re.IGNORECASE),
]

# Self-harm patterns
SELF_HARM_PATTERNS = [
    re.compile(r"\b(how\s+to\s+)?(commit\s+)?suicide\b", re.IGNORECASE),
    re.compile(r"\bself[- ]harm\b", re.IGNORECASE),
    re.compile(r"\b(cut|hurt)\s+yourself\b", re.IGNORECASE),
]

# CSAM patterns
CSAM_PATTERNS = [
    re.compile(r"\b(child|minor|underage)\s+(porn|sexual|nude)\b", re.IGNORECASE),
    re.compile(r"\bpedophil\w*\b", re.IGNORECASE),
]

# Violence/gore patterns
VIOLENCE_PATTERNS = [
    re.compile(r"\b(graphic|detailed)\s+(violence|murder|torture)\b", re.IGNORECASE),
    re.compile(r"\b(how\s+to\s+)?(make|build)\s+(bomb|weapon|explosive)\b", re.IGNORECASE),
]


def scan_content(text: str) -> dict[str, Any]:
    """Scan content for toxic patterns."""
    results: dict[str, Any] = {
        "hate_speech": [],
        "self_harm": [],
        "csam": [],
        "violence": [],
        "total_flags": 0,
    }
    
    for pattern in HATE_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            results["hate_speech"].extend(matches[:3])
    
    for pattern in SELF_HARM_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            results["self_harm"].extend(matches[:3])
    
    for pattern in CSAM_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            results["csam"].extend(matches[:3])
    
    for pattern in VIOLENCE_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            results["violence"].extend(matches[:3])
    
    results["total_flags"] = (
        len(results["hate_speech"]) +
        len(results["self_harm"]) +
        len(results["csam"]) +
        len(results["violence"])
    )
    
    return results


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    """Run toxicity scan check.
    
    Config options:
        categories: List of categories to check [hate_speech, self_harm, csam, violence]
        action_on_detect: Action when toxicity detected (default: reject)
        flag_threshold: Number of flags before action (default: 1)
    """
    text = record.get("text", "") or record.get("content", "") or ""
    
    if not text:
        return CheckResult(passed=True, action="keep", reason="no_content")
    
    categories = config.get("categories", ["hate_speech", "self_harm", "csam", "violence"])
    action_on_detect = config.get("action_on_detect", "reject")
    flag_threshold = config.get("flag_threshold", 1)
    
    scan_results = scan_content(text)
    
    relevant_flags = sum(
        len(scan_results[cat]) for cat in categories if cat in scan_results
    )
    
    details = {
        "scan_results": {k: v for k, v in scan_results.items() if k in categories or k == "total_flags"},
        "relevant_flags": relevant_flags,
    }
    
    # CSAM is always a hard reject
    if scan_results["csam"]:
        return CheckResult(
            passed=False, action="reject", reason="csam_detected",
            details=details, confidence=1.0,
        )
    
    if relevant_flags >= flag_threshold:
        flagged_categories = [cat for cat in categories if scan_results.get(cat)]
        return CheckResult(
            passed=False, action=action_on_detect,
            reason=f"toxicity_detected: {', '.join(flagged_categories)}",
            details=details, confidence=0.9,
        )
    
    return CheckResult(passed=True, action="keep", details=details, confidence=0.95)


__all__ = ["check_name", "check", "scan_content"]
