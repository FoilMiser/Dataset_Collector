"""Chemistry-specific yellow screening with dual-use detection.

This module provides chemistry-specific filtering for the yellow screening stage.
It includes:
- Controlled substance synthesis detection
- CAS Registry Number extraction and validation
- Quality indicator scoring for legitimate research content
- Dual-use chemical content flagging
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

# CAS Registry Number pattern (format: XXXXXXX-XX-X)
CAS_PATTERN = re.compile(r"\b(\d{2,7})-(\d{2})-(\d)\b")

# Controlled substance synthesis patterns
CONTROLLED_PATTERNS = [
    re.compile(
        r"\b(synthesis|synthesize|prepare|preparation|route)\b"
        r".{0,100}"
        r"\b(fentanyl|methamphetamine|mdma|lsd|heroin|cocaine)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(nerve\s+agent|chemical\s+weapon|mustard\s+gas|sarin|tabun|soman|vx|novichok)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(synthesis|prepare|make|manufacture)\b"
        r".{0,50}"
        r"\b(explosive|detonator|rdx|petn|tatp|hmtd)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(weaponize|weapon|attack|poison)\b"
        r".{0,50}"
        r"\b(chlorine|phosgene|hydrogen\s+cyanide|ricin)\b",
        re.IGNORECASE,
    ),
]

QUALITY_INDICATORS = [
    "peer-reviewed", "peer reviewed", "crystallographic", "spectroscopic",
    "computational chemistry", "density functional theory", "dft calculation",
    "molecular dynamics", "quantum chemistry", "ab initio", "nmr spectr",
    "mass spectr", "x-ray diffraction", "xrd", "ftir", "raman",
]


def validate_cas_number(cas_string: str) -> bool:
    """Validate CAS Registry Number checksum."""
    match = CAS_PATTERN.match(cas_string)
    if not match:
        return False
    digits = match.group(1) + match.group(2) + match.group(3)
    check_digit = int(digits[-1])
    total = sum(int(d) * (i + 1) for i, d in enumerate(reversed(digits[:-1])))
    return (total % 10) == check_digit


def extract_cas_numbers(text: str) -> list[dict[str, Any]]:
    """Extract and validate CAS numbers from text."""
    results = []
    seen: set[str] = set()
    for match in CAS_PATTERN.finditer(text):
        cas = match.group(0)
        if cas not in seen:
            seen.add(cas)
            results.append({"cas_number": cas, "is_valid": validate_cas_number(cas)})
    return results


def compute_quality_score(text: str) -> tuple[int, list[str]]:
    """Compute quality score based on research indicators."""
    text_lower = text.lower()
    matched = [ind for ind in QUALITY_INDICATORS if ind in text_lower]
    return len(matched), matched


def check_controlled_content(text: str) -> tuple[bool, str | None]:
    """Check for controlled substance synthesis content."""
    for pattern in CONTROLLED_PATTERNS:
        match = pattern.search(text)
        if match:
            return True, match.group(0)[:100]
    return False, None


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Chemistry-specific filtering with dual-use screening."""
    text = raw.get("text", "") or raw.get("abstract", "") or raw.get("content", "") or ""

    has_controlled, controlled_match = check_controlled_content(text)
    if has_controlled:
        return FilterDecision(
            allow=False, reason="controlled_substance_content",
            text=text[:500] if text else None,
            extra={"rejection_type": "dual_use", "matched_content": controlled_match},
        )

    cas_numbers = extract_cas_numbers(text)
    valid_cas_count = sum(1 for cas in cas_numbers if cas["is_valid"])
    quality_score, quality_matches = compute_quality_score(text)

    decision = standard_filter(raw, ctx)
    decision.extra = decision.extra or {}
    decision.extra.update({
        "cas_numbers_found": len(cas_numbers),
        "cas_numbers_valid": valid_cas_count,
        "cas_numbers": cas_numbers[:10],
        "quality_score": quality_score,
        "quality_indicators": quality_matches,
    })

    if quality_score >= 3 and decision.allow:
        decision.extra["quality_boost"] = True
    return decision


def transform_record(
    raw: dict[str, Any], ctx: DomainContext, decision: FilterDecision, *, license_profile: str,
) -> dict[str, Any] | None:
    """Transform chemistry record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if result is None:
        return None
    extra = decision.extra or {}
    if extra.get("cas_numbers"):
        result["extracted_cas_numbers"] = [c["cas_number"] for c in extra["cas_numbers"] if c["is_valid"]]
    if extra.get("quality_score"):
        result["research_quality_score"] = extra["quality_score"]
    return result


__all__ = ["filter_record", "transform_record", "validate_cas_number", "extract_cas_numbers"]
