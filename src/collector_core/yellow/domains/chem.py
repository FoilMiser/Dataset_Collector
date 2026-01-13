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
# These patterns detect potential instructions for synthesizing dangerous chemicals
CONTROLLED_PATTERNS = [
    # Drug synthesis
    re.compile(
        r"\b(synthesis|synthesize|prepare|preparation|route)\b"
        r".{0,100}"
        r"\b(fentanyl|methamphetamine|mdma|lsd|heroin|cocaine)\b",
        re.IGNORECASE,
    ),
    # Chemical weapons / nerve agents
    re.compile(
        r"\b(nerve\s+agent|chemical\s+weapon|mustard\s+gas|sarin|tabun|soman|vx|novichok)\b",
        re.IGNORECASE,
    ),
    # Explosives precursors
    re.compile(
        r"\b(synthesis|prepare|make|manufacture)\b"
        r".{0,50}"
        r"\b(explosive|detonator|rdx|petn|tatp|hmtd)\b",
        re.IGNORECASE,
    ),
    # Toxic industrial chemicals misuse
    re.compile(
        r"\b(weaponize|weapon|attack|poison)\b"
        r".{0,50}"
        r"\b(chlorine|phosgene|hydrogen\s+cyanide|ricin)\b",
        re.IGNORECASE,
    ),
]

# Quality indicators that suggest legitimate research content
QUALITY_INDICATORS = [
    "peer-reviewed",
    "peer reviewed",
    "crystallographic",
    "spectroscopic",
    "computational chemistry",
    "density functional theory",
    "dft calculation",
    "molecular dynamics",
    "quantum chemistry",
    "ab initio",
    "nmr spectr",
    "mass spectr",
    "x-ray diffraction",
    "xrd",
    "ftir",
    "raman",
]

# Safety-related positive indicators
SAFETY_INDICATORS = [
    "safety data sheet",
    "sds",
    "msds",
    "hazard classification",
    "ghs",
    "exposure limit",
    "ppe requirement",
    "toxicological",
    "environmental impact",
]


def validate_cas_number(cas_string: str) -> bool:
    """Validate CAS Registry Number checksum.

    CAS numbers use a checksum digit calculated as:
    sum of (digit * position from right) mod 10

    Args:
        cas_string: CAS number in format XXXXXXX-XX-X

    Returns:
        True if valid CAS number
    """
    match = CAS_PATTERN.match(cas_string)
    if not match:
        return False

    # Remove hyphens and get digits
    digits = match.group(1) + match.group(2) + match.group(3)
    check_digit = int(digits[-1])

    # Calculate checksum
    total = 0
    for i, digit in enumerate(reversed(digits[:-1])):
        total += int(digit) * (i + 1)

    return (total % 10) == check_digit


def extract_cas_numbers(text: str) -> list[dict[str, Any]]:
    """Extract and validate CAS numbers from text.

    Args:
        text: Text to search for CAS numbers

    Returns:
        List of dicts with cas_number and is_valid keys
    """
    results = []
    seen: set[str] = set()

    for match in CAS_PATTERN.finditer(text):
        cas = match.group(0)
        if cas not in seen:
            seen.add(cas)
            results.append({
                "cas_number": cas,
                "is_valid": validate_cas_number(cas),
            })

    return results


def compute_quality_score(text: str) -> tuple[float, list[str]]:
    """Compute quality score based on research indicators.

    Args:
        text: Text to analyze

    Returns:
        Tuple of (score, list of matched indicators)
    """
    text_lower = text.lower()
    matched = []

    for indicator in QUALITY_INDICATORS:
        if indicator in text_lower:
            matched.append(indicator)

    for indicator in SAFETY_INDICATORS:
        if indicator in text_lower:
            matched.append(indicator)

    # Score based on number of matches, normalized
    max_score = len(QUALITY_INDICATORS) + len(SAFETY_INDICATORS)
    score = len(matched) / max_score if max_score > 0 else 0.0

    return score, matched


def check_controlled_content(text: str) -> tuple[bool, str | None]:
    """Check for controlled substance synthesis content.

    Args:
        text: Text to check

    Returns:
        Tuple of (has_controlled_content, matched_pattern_description)
    """
    for pattern in CONTROLLED_PATTERNS:
        match = pattern.search(text)
        if match:
            return True, match.group(0)[:100]  # Truncate for logging

    return False, None


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Chemistry-specific filtering with dual-use screening.

    This function extends the standard filter with chemistry-specific checks:
    1. Controlled substance synthesis detection (reject)
    2. CAS number extraction and validation
    3. Quality indicator scoring

    Args:
        raw: Raw record to filter
        ctx: Domain context with configuration

    Returns:
        FilterDecision with chemistry-specific metadata
    """
    # Get text content
    text = (
        raw.get("text", "") or
        raw.get("abstract", "") or
        raw.get("content", "") or
        raw.get("body", "") or
        ""
    )

    # Check for controlled substance content first (hard reject)
    has_controlled, controlled_match = check_controlled_content(text)
    if has_controlled:
        return FilterDecision(
            allow=False,
            reason="controlled_substance_content",
            text=text[:500] if text else None,
            extra={
                "rejection_type": "dual_use",
                "matched_content": controlled_match,
            },
        )

    # Extract CAS numbers
    cas_numbers = extract_cas_numbers(text)
    valid_cas_count = sum(1 for cas in cas_numbers if cas["is_valid"])

    # Compute quality score
    quality_score, quality_matches = compute_quality_score(text)

    # Run standard filter
    decision = standard_filter(raw, ctx)

    # Enhance decision with chemistry-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update({
        "cas_numbers_found": len(cas_numbers),
        "cas_numbers_valid": valid_cas_count,
        "cas_numbers": cas_numbers[:10],  # Limit to first 10
        "quality_score": quality_score,
        "quality_indicators": quality_matches,
    })

    # Boost allow probability for high-quality research content
    if quality_score >= 0.15 and decision.allow:
        decision.extra["quality_boost"] = True

    return decision


def transform_record(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    """Transform chemistry record with domain-specific fields.

    Adds chemistry-specific fields to the output record:
    - cas_numbers: Extracted CAS Registry Numbers
    - quality_score: Research quality indicator score

    Args:
        raw: Raw input record
        ctx: Domain context
        decision: Filter decision
        license_profile: License profile for the record

    Returns:
        Transformed record or None if should be excluded
    """
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)

    if result is None:
        return None

    # Add chemistry-specific fields
    extra = decision.extra or {}
    if extra.get("cas_numbers"):
        result["extracted_cas_numbers"] = [
            cas["cas_number"] for cas in extra["cas_numbers"] if cas["is_valid"]
        ]

    if extra.get("quality_score"):
        result["research_quality_score"] = extra["quality_score"]

    return result


__all__ = [
    "filter_record",
    "transform_record",
    "validate_cas_number",
    "extract_cas_numbers",
]
