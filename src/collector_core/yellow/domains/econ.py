"""Economics/statistics-specific yellow screening.

This module provides economics and statistics-specific filtering including:
- Financial data sensitivity detection
- PII in economic data detection
- Temporal data validation
- Statistical methodology indicators
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

# Financial data sensitivity patterns
FINANCIAL_SENSITIVITY_PATTERNS = {
    "account_number": re.compile(r"\b(account|acct)\s*(number|#|no)\s*[:\s]?\d{8,17}\b", re.IGNORECASE),
    "routing_number": re.compile(r"\b(routing|aba)\s*(number|#)\s*[:\s]?\d{9}\b", re.IGNORECASE),
    "credit_card": re.compile(r"\b(?:\d{4}[-.\s]?){3}\d{4}\b"),
    "tax_id": re.compile(r"\b(tin|ein|tax\s*id)\s*[:\s]?\d{2}[-]?\d{7}\b", re.IGNORECASE),
    "salary": re.compile(r"\b(salary|compensation|wage)\s*[:\s]?\$?[\d,]+(?:\.\d{2})?\b", re.IGNORECASE),
}

# PII in economic data
ECONOMIC_PII_PATTERNS = {
    "ssn": re.compile(r"\b\d{3}[-.\s]?\d{2}[-.\s]?\d{4}\b"),
    "individual_name": re.compile(r"\b(respondent|participant|subject)\s*[:\s]?[A-Z][a-z]+\s+[A-Z][a-z]+\b"),
    "address": re.compile(r"\b\d+\s+\w+\s+(street|st|avenue|ave|road|rd)\b", re.IGNORECASE),
}

# Temporal data patterns
TEMPORAL_PATTERNS = {
    "year": re.compile(r"\b(19|20)\d{2}\b"),
    "quarter": re.compile(r"\b(Q[1-4]|[1-4]Q)\s*(19|20)?\d{2}\b", re.IGNORECASE),
    "month_year": re.compile(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s*(19|20)?\d{2}\b",
        re.IGNORECASE,
    ),
    "date_range": re.compile(r"\b(19|20)\d{2}\s*[-–]\s*(19|20)?\d{2,4}\b"),
}

# Statistical methodology indicators
METHODOLOGY_INDICATORS = [
    "regression",
    "correlation",
    "variance",
    "standard deviation",
    "confidence interval",
    "p-value",
    "significance",
    "hypothesis",
    "sample size",
    "population",
    "survey",
    "census",
    "panel data",
    "time series",
    "cross-sectional",
    "longitudinal",
    "econometric",
]

# Data source quality indicators
QUALITY_INDICATORS = [
    "federal reserve",
    "bureau of labor statistics",
    "bls",
    "census bureau",
    "world bank",
    "imf",
    "oecd",
    "eurostat",
    "peer-reviewed",
    "official statistics",
]


def detect_financial_sensitivity(text: str) -> dict[str, int]:
    """Detect financially sensitive data patterns."""
    results = {}
    for sens_type, pattern in FINANCIAL_SENSITIVITY_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            results[sens_type] = len(matches)
    return results


def detect_economic_pii(text: str) -> dict[str, int]:
    """Detect PII specific to economic data."""
    results = {}
    for pii_type, pattern in ECONOMIC_PII_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            results[pii_type] = len(matches)
    return results


def analyze_temporal_coverage(text: str) -> dict[str, Any]:
    """Analyze temporal coverage of the data."""
    years = TEMPORAL_PATTERNS["year"].findall(text)
    quarters = TEMPORAL_PATTERNS["quarter"].findall(text)
    date_ranges = TEMPORAL_PATTERNS["date_range"].findall(text)

    year_ints = [int(y) for y in years if y.isdigit() or len(y) == 4]

    return {
        "years_mentioned": sorted(set(year_ints)) if year_ints else [],
        "year_range": (min(year_ints), max(year_ints)) if year_ints else None,
        "quarter_references": len(quarters),
        "date_range_references": len(date_ranges),
    }


def check_methodology_indicators(text: str) -> tuple[int, list[str]]:
    """Check for statistical methodology indicators."""
    text_lower = text.lower()
    matched = [ind for ind in METHODOLOGY_INDICATORS if ind in text_lower]
    return len(matched), matched


def check_quality_indicators(text: str) -> tuple[int, list[str]]:
    """Check for data source quality indicators."""
    text_lower = text.lower()
    matched = [ind for ind in QUALITY_INDICATORS if ind in text_lower]
    return len(matched), matched


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Economics/statistics-specific filtering."""
    text = (
        raw.get("text", "")
        or raw.get("abstract", "")
        or raw.get("content", "")
        or raw.get("description", "")
        or ""
    )

    # Detect financial sensitivity
    financial_sensitive = detect_financial_sensitivity(text)

    # Detect PII
    pii_found = detect_economic_pii(text)

    # Analyze temporal coverage
    temporal_info = analyze_temporal_coverage(text)

    # Check methodology
    methodology_score, methodology_matches = check_methodology_indicators(text)

    # Check quality
    quality_score, quality_matches = check_quality_indicators(text)

    # Run standard filter
    decision = standard_filter(raw, ctx)

    # Add economics-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update(
        {
            "financial_sensitive": financial_sensitive,
            "financial_sensitivity_count": sum(financial_sensitive.values()),
            "pii_detected": pii_found,
            "pii_count": sum(pii_found.values()),
            "temporal_coverage": temporal_info,
            "methodology_score": methodology_score,
            "methodology_indicators": methodology_matches,
            "quality_score": quality_score,
            "quality_indicators": quality_matches,
        }
    )

    # Flag if sensitive financial data detected
    if financial_sensitive:
        decision.extra["has_financial_sensitive"] = True

    # Flag if PII detected
    if pii_found:
        decision.extra["has_pii"] = True

    return decision


def transform_record(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    """Transform economics record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if result is None:
        return None

    extra = decision.extra or {}

    if extra.get("temporal_coverage"):
        result["temporal_coverage"] = extra["temporal_coverage"]

    if extra.get("methodology_score"):
        result["methodology_score"] = extra["methodology_score"]

    if extra.get("quality_score"):
        result["data_quality_score"] = extra["quality_score"]

    if extra.get("has_pii") or extra.get("has_financial_sensitive"):
        result["_sensitive_data_detected"] = True

    return result


__all__ = ["filter_record", "transform_record", "detect_financial_sensitivity"]
