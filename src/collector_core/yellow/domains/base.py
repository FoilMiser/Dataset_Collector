"""Base domain implementations and shared utilities.

This module provides:
1. Default filter_record and transform_record implementations that delegate
   to standard_filter and standard_transform
2. Common utility functions for text extraction, PII detection, etc.
3. Type annotations and documentation for domain module authors

Domain modules can inherit from this or import utilities directly.
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

# Re-export base types for convenience
__all__ = [
    # Base types
    "DomainContext",
    "FilterDecision",
    "standard_filter",
    "standard_transform",
    # Default implementations
    "filter_record",
    "transform_record",
    # Utilities
    "extract_text",
    "detect_pii",
    "detect_email_patterns",
    "detect_phone_patterns",
    "detect_ssn_patterns",
    "calculate_quality_score",
]


# =============================================================================
# Default implementations
# =============================================================================


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Default filter implementation - delegates to standard_filter.

    Domain modules can override this to add domain-specific filtering logic.
    The typical pattern is:
    1. Extract domain-specific text/features
    2. Perform domain-specific rejection checks (hard rejects)
    3. Call standard_filter(raw, ctx) to get base decision
    4. Add domain-specific metadata to decision.extra
    5. Return the decision

    Args:
        raw: Raw record dict to filter
        ctx: Domain context with configuration and state

    Returns:
        FilterDecision indicating allow/reject with reason and metadata
    """
    return standard_filter(raw, ctx)


def transform_record(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    """Default transform implementation - delegates to standard_transform.

    Domain modules can override this to add domain-specific output fields.
    The typical pattern is:
    1. Call standard_transform to get base result
    2. Return None if standard_transform returned None
    3. Add domain-specific screening metadata
    4. Add domain-specific result fields
    5. Return the result

    Args:
        raw: Raw record dict to transform
        ctx: Domain context with configuration and state
        decision: FilterDecision from filter_record
        license_profile: License profile name for this record

    Returns:
        Transformed record dict, or None if record should be excluded
    """
    return standard_transform(raw, ctx, decision, license_profile=license_profile)


# =============================================================================
# Text extraction utilities
# =============================================================================


def extract_text(
    raw: dict[str, Any],
    field_candidates: list[str] | None = None,
) -> str:
    """Extract text content from raw record, trying multiple field names.

    Args:
        raw: Raw record dict
        field_candidates: List of field names to try, in priority order.
            Defaults to ["text", "content", "body", "abstract"]

    Returns:
        Extracted text content, or empty string if none found
    """
    if field_candidates is None:
        field_candidates = ["text", "content", "body", "abstract"]

    for field in field_candidates:
        value = raw.get(field)
        if value and isinstance(value, str):
            return value

    return ""


# =============================================================================
# PII detection utilities
# =============================================================================


# Common PII patterns
_EMAIL_PATTERN = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
)

_PHONE_PATTERN = re.compile(
    r"\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b"
)

_SSN_PATTERN = re.compile(
    r"\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b"
)


def detect_email_patterns(text: str) -> list[str]:
    """Detect email address patterns in text.

    Args:
        text: Text to scan for email patterns

    Returns:
        List of matched email addresses (may contain false positives)
    """
    return _EMAIL_PATTERN.findall(text)


def detect_phone_patterns(text: str) -> list[str]:
    """Detect phone number patterns in text.

    Args:
        text: Text to scan for phone patterns

    Returns:
        List of matched phone numbers (may contain false positives)
    """
    return _PHONE_PATTERN.findall(text)


def detect_ssn_patterns(text: str) -> list[str]:
    """Detect SSN-like patterns in text.

    Note: This can have high false positive rates for data that
    contains similar number patterns.

    Args:
        text: Text to scan for SSN patterns

    Returns:
        List of matched SSN-like patterns
    """
    return _SSN_PATTERN.findall(text)


def detect_pii(text: str) -> dict[str, Any]:
    """Detect various PII patterns in text.

    This is a convenience function that runs all PII detectors.

    Args:
        text: Text to scan for PII

    Returns:
        Dict with keys 'emails', 'phones', 'ssns', 'has_pii', 'pii_count'
    """
    emails = detect_email_patterns(text)
    phones = detect_phone_patterns(text)
    ssns = detect_ssn_patterns(text)

    total_count = len(emails) + len(phones) + len(ssns)

    return {
        "emails": emails,
        "phones": phones,
        "ssns": ssns,
        "has_pii": total_count > 0,
        "pii_count": total_count,
    }


# =============================================================================
# Quality scoring utilities
# =============================================================================


def calculate_quality_score(
    signals: dict[str, float],
    weights: dict[str, float] | None = None,
) -> float:
    """Calculate weighted quality score from multiple signals.

    Args:
        signals: Dict of signal_name -> value (0.0-1.0 range expected)
        weights: Optional dict of signal_name -> weight. Missing weights default to 1.0.

    Returns:
        Weighted average quality score (0.0-1.0 range)
    """
    if not signals:
        return 0.0

    weights = weights or {}
    total_weight = 0.0
    weighted_sum = 0.0

    for signal_name, value in signals.items():
        weight = weights.get(signal_name, 1.0)
        weighted_sum += value * weight
        total_weight += weight

    if total_weight == 0.0:
        return 0.0

    return weighted_sum / total_weight
