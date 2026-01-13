"""Code-specific yellow screening with license and security checks.

This module provides code-specific filtering including:
- License header extraction and validation
- Secret/credential detection
- Malware pattern detection
- Code quality assessment
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

# SPDX License Identifier pattern
SPDX_PATTERN = re.compile(
    r"SPDX-License-Identifier:\s*([A-Za-z0-9\-\.+]+(?:\s+(?:AND|OR|WITH)\s+[A-Za-z0-9\-\.+]+)*)",
    re.IGNORECASE,
)

# License header patterns
LICENSE_PATTERNS = {
    "MIT": re.compile(
        r"(?:MIT License|Permission is hereby granted,?\s+free of charge)",
        re.IGNORECASE,
    ),
    "Apache-2.0": re.compile(
        r"(?:Apache License.*Version 2\.0|Licensed under the Apache License)",
        re.IGNORECASE,
    ),
    "GPL-3.0": re.compile(
        r"(?:GNU General Public License.*(?:version\s+)?3|GPLv3)",
        re.IGNORECASE,
    ),
    "GPL-2.0": re.compile(
        r"(?:GNU General Public License.*(?:version\s+)?2|GPLv2)",
        re.IGNORECASE,
    ),
    "BSD-3-Clause": re.compile(
        r"(?:BSD 3-Clause|three conditions|Redistributions? of source code)",
        re.IGNORECASE,
    ),
    "BSD-2-Clause": re.compile(
        r"(?:BSD 2-Clause|Simplified BSD|two conditions)",
        re.IGNORECASE,
    ),
    "LGPL": re.compile(
        r"(?:GNU Lesser General Public License|LGPL)",
        re.IGNORECASE,
    ),
    "MPL-2.0": re.compile(
        r"(?:Mozilla Public License.*2\.0|MPL-2\.0)",
        re.IGNORECASE,
    ),
    "Unlicense": re.compile(
        r"(?:This is free and unencumbered software|Unlicense)",
        re.IGNORECASE,
    ),
    "CC0-1.0": re.compile(
        r"(?:CC0|Creative Commons Zero|Public Domain Dedication)",
        re.IGNORECASE,
    ),
}

# Secret/credential patterns
SECRET_PATTERNS = [
    # API keys
    (
        re.compile(
            r"\b(?:api[_-]?key|apikey)\s*[:=]\s*['\"]?([a-zA-Z0-9_\-]{20,})['\"]?",
            re.IGNORECASE,
        ),
        "api_key",
    ),
    # AWS credentials
    (re.compile(r"\b(?:AKIA|ABIA|ACCA|ASIA)[A-Z0-9]{16}\b"), "aws_access_key"),
    (
        re.compile(
            r"\baws[_-]?secret[_-]?access[_-]?key\s*[:=]\s*['\"]?([a-zA-Z0-9/+=]{40})['\"]?",
            re.IGNORECASE,
        ),
        "aws_secret_key",
    ),
    # Private keys
    (
        re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"),
        "private_key",
    ),
    # Passwords in config
    (
        re.compile(
            r"\b(?:password|passwd|pwd)\s*[:=]\s*['\"]([^'\"]{8,})['\"]",
            re.IGNORECASE,
        ),
        "password",
    ),
    # Database connection strings
    (
        re.compile(r"(?:mysql|postgres|mongodb)://[^:]+:[^@]+@", re.IGNORECASE),
        "database_url",
    ),
    # JWT tokens
    (
        re.compile(r"\beyJ[a-zA-Z0-9_-]*\.eyJ[a-zA-Z0-9_-]*\.[a-zA-Z0-9_-]*\b"),
        "jwt_token",
    ),
    # GitHub tokens
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[a-zA-Z0-9]{36}\b"), "github_token"),
]

# Malware/exploit patterns
MALWARE_PATTERNS = [
    # Shell injection
    re.compile(
        r"(?:;\s*(?:rm|wget|curl|nc|bash|sh)\s+-|`.*`|\$\(.*\))", re.IGNORECASE
    ),
    # Eval with user input
    re.compile(r"\beval\s*\(\s*(?:request|input|argv|params)", re.IGNORECASE),
    # SQL injection vectors
    re.compile(r"(?:UNION\s+SELECT|OR\s+1\s*=\s*1|'\s*OR\s+')", re.IGNORECASE),
    # Known malware signatures
    re.compile(r"(?:mimikatz|metasploit|cobalt\s*strike|beacon)", re.IGNORECASE),
]

# Code quality indicators
QUALITY_INDICATORS = [
    ("has_tests", re.compile(r"(?:def test_|class Test|@pytest|unittest)", re.IGNORECASE)),
    ("has_docstrings", re.compile(r'"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'')),
    ("has_type_hints", re.compile(r"def\s+\w+\([^)]*:\s*\w+[^)]*\)\s*(?:->|:)")),
    ("has_logging", re.compile(r"(?:logging\.|logger\.|log\.)", re.IGNORECASE)),
    ("has_error_handling", re.compile(r"(?:try:|except:|raise\s+\w+)")),
]


def extract_license_info(text: str, max_lines: int = 100) -> dict[str, Any]:
    """Extract license information from code header.

    Args:
        text: Code content
        max_lines: Maximum lines to scan for license header

    Returns:
        Dict with license detection results
    """
    # Look at first N lines for license
    lines = text.split("\n")[:max_lines]
    header = "\n".join(lines)

    result: dict[str, Any] = {
        "has_license_header": False,
        "detected_spdx": None,
        "detected_license": None,
        "confidence": 0.0,
    }

    # Try SPDX identifier first (highest confidence)
    spdx_match = SPDX_PATTERN.search(header)
    if spdx_match:
        result["has_license_header"] = True
        result["detected_spdx"] = spdx_match.group(1)
        result["confidence"] = 1.0
        return result

    # Try pattern matching
    for license_id, pattern in LICENSE_PATTERNS.items():
        if pattern.search(header):
            result["has_license_header"] = True
            result["detected_license"] = license_id
            result["confidence"] = 0.8
            return result

    return result


def detect_secrets(text: str) -> list[dict[str, Any]]:
    """Detect potential secrets/credentials in code.

    Args:
        text: Code content

    Returns:
        List of detected secret locations (without actual values)
    """
    findings = []

    for pattern, secret_type in SECRET_PATTERNS:
        for match in pattern.finditer(text):
            # Find line number
            line_start = text.rfind("\n", 0, match.start()) + 1
            line_num = text.count("\n", 0, match.start()) + 1

            findings.append({
                "type": secret_type,
                "line": line_num,
                "column": match.start() - line_start,
                # Don't include actual secret value
            })

    return findings


def detect_malware_patterns(text: str) -> list[dict[str, Any]]:
    """Detect potential malware/exploit patterns.

    Args:
        text: Code content

    Returns:
        List of suspicious pattern matches
    """
    findings = []

    for pattern in MALWARE_PATTERNS:
        for match in pattern.finditer(text):
            line_num = text.count("\n", 0, match.start()) + 1
            findings.append({
                "pattern": pattern.pattern[:50],
                "line": line_num,
                "matched": match.group(0)[:50],
            })

    return findings


def assess_code_quality(text: str) -> dict[str, Any]:
    """Assess code quality indicators.

    Args:
        text: Code content

    Returns:
        Dict with quality assessments
    """
    result: dict[str, Any] = {"quality_score": 0}

    for indicator_name, pattern in QUALITY_INDICATORS:
        has_indicator = bool(pattern.search(text))
        result[indicator_name] = has_indicator
        if has_indicator:
            result["quality_score"] += 1

    return result


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Code-specific filtering with license and security checks.

    Args:
        raw: Raw record to filter
        ctx: Domain context

    Returns:
        FilterDecision with code-specific metadata
    """
    text = (
        raw.get("content", "") or
        raw.get("code", "") or
        raw.get("text", "") or
        ""
    )

    # Detect malware patterns (hard reject)
    malware_findings = detect_malware_patterns(text)
    if malware_findings:
        return FilterDecision(
            allow=False,
            reason="malware_pattern_detected",
            extra={
                "rejection_type": "security",
                "malware_findings": malware_findings[:5],
            },
        )

    # Detect secrets (flag for review but don't auto-reject)
    secret_findings = detect_secrets(text)

    # Extract license info
    license_info = extract_license_info(text)

    # Assess code quality
    quality_info = assess_code_quality(text)

    # Run standard filter
    decision = standard_filter(raw, ctx)

    # Flag if secrets detected
    if secret_findings:
        decision.reason = decision.reason or ""
        if decision.reason:
            decision.reason += "; "
        decision.reason += "secrets_detected"

    # Add code-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update({
        "license_info": license_info,
        "secrets_found": len(secret_findings),
        "secret_types": list(set(s["type"] for s in secret_findings)),
        "quality_info": quality_info,
    })

    return decision


def transform_record(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    """Transform code record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)

    if result is None:
        return None

    # Add screening metadata
    screening: dict[str, Any] = {"domain": "code"}
    if decision.extra:
        screening.update(decision.extra)
    result["screening"] = screening

    extra = decision.extra or {}

    # Add extracted license
    license_info = extra.get("license_info", {})
    if license_info.get("detected_spdx"):
        result["detected_spdx"] = license_info["detected_spdx"]
    elif license_info.get("detected_license"):
        result["detected_license"] = license_info["detected_license"]

    # Add quality metrics
    if extra.get("quality_info"):
        result["code_quality"] = extra["quality_info"]

    # Flag if secrets were found (content should be scrubbed separately)
    if extra.get("secrets_found", 0) > 0:
        result["_secrets_detected"] = True

    return result


__all__ = ["filter_record", "transform_record"]
