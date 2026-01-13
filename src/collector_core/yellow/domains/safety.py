"""Safety incident-specific yellow screening.

This module provides safety incident-specific filtering including:
- Incident type classification
- PII in incident reports detection
- Severity assessment
- Regulatory compliance indicators
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

# Incident type patterns
INCIDENT_TYPE_PATTERNS = {
    "workplace_injury": re.compile(
        r"\b(workplace|occupational|work-related)\s+(injury|accident|incident)\b",
        re.IGNORECASE,
    ),
    "equipment_failure": re.compile(
        r"\b(equipment|machinery|machine)\s+(failure|malfunction|breakdown)\b",
        re.IGNORECASE,
    ),
    "chemical_spill": re.compile(
        r"\b(chemical|hazardous|toxic)\s+(spill|release|leak)\b",
        re.IGNORECASE,
    ),
    "fire_incident": re.compile(
        r"\b(fire|explosion|combustion)\s+(incident|event|occurrence)\b",
        re.IGNORECASE,
    ),
    "transportation": re.compile(
        r"\b(vehicle|transport|traffic)\s+(accident|collision|crash)\b",
        re.IGNORECASE,
    ),
    "fall": re.compile(r"\b(fall|fell|falling)\s+from\s+(height|ladder|scaffold)\b", re.IGNORECASE),
    "electrical": re.compile(
        r"\b(electrical|electrocution|shock)\s+(incident|injury|hazard)\b",
        re.IGNORECASE,
    ),
}

# Severity indicators
SEVERITY_KEYWORDS = {
    "fatal": ["fatal", "fatality", "death", "deceased", "killed"],
    "serious": ["serious", "severe", "critical", "hospitalized", "amputation", "permanent"],
    "moderate": ["moderate", "medical treatment", "lost time", "restricted duty"],
    "minor": ["minor", "first aid", "no lost time", "near miss"],
}

# PII patterns specific to incident reports
INCIDENT_PII_PATTERNS = {
    "person_name": re.compile(
        r"\b(Mr\.|Mrs\.|Ms\.|Dr\.)\s+[A-Z][a-z]+\s+[A-Z][a-z]+\b"
    ),
    "employee_id": re.compile(r"\b(employee|worker|staff)\s*(id|number|#)\s*[:\s]?\d+\b", re.IGNORECASE),
    "date_of_birth": re.compile(r"\b(dob|date of birth|born)\s*[:\s]?\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", re.IGNORECASE),
    "address": re.compile(r"\b\d+\s+\w+\s+(street|st|avenue|ave|road|rd|drive|dr)\b", re.IGNORECASE),
}

# Regulatory compliance indicators
COMPLIANCE_INDICATORS = [
    "osha",
    "recordable",
    "ntsb",
    "faa",
    "msha",
    "csa",
    "regulation",
    "compliance",
    "violation",
    "citation",
    "inspection",
    "audit",
]


def classify_incident_type(text: str) -> list[str]:
    """Classify incident into types based on content."""
    types = []
    for incident_type, pattern in INCIDENT_TYPE_PATTERNS.items():
        if pattern.search(text):
            types.append(incident_type)
    return types


def assess_severity(text: str) -> tuple[str, list[str]]:
    """Assess incident severity based on keywords."""
    text_lower = text.lower()

    for severity, keywords in SEVERITY_KEYWORDS.items():
        matched = [kw for kw in keywords if kw in text_lower]
        if matched:
            return severity, matched

    return "unknown", []


def detect_incident_pii(text: str) -> dict[str, int]:
    """Detect PII specific to incident reports."""
    results = {}
    for pii_type, pattern in INCIDENT_PII_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            results[pii_type] = len(matches)
    return results


def check_compliance_indicators(text: str) -> tuple[int, list[str]]:
    """Check for regulatory compliance indicators."""
    text_lower = text.lower()
    matched = [ind for ind in COMPLIANCE_INDICATORS if ind in text_lower]
    return len(matched), matched


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Safety incident-specific filtering."""
    text = (
        raw.get("text", "")
        or raw.get("description", "")
        or raw.get("narrative", "")
        or raw.get("content", "")
        or ""
    )

    # Classify incident type
    incident_types = classify_incident_type(text)

    # Assess severity
    severity, severity_keywords = assess_severity(text)

    # Detect PII
    pii_found = detect_incident_pii(text)

    # Check compliance indicators
    compliance_score, compliance_matches = check_compliance_indicators(text)

    # Run standard filter
    decision = standard_filter(raw, ctx)

    # Add safety-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update(
        {
            "incident_types": incident_types,
            "severity": severity,
            "severity_keywords": severity_keywords,
            "pii_detected": pii_found,
            "pii_count": sum(pii_found.values()),
            "compliance_score": compliance_score,
            "compliance_indicators": compliance_matches,
        }
    )

    # Flag high severity incidents
    if severity in ("fatal", "serious"):
        decision.extra["high_severity"] = True

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
    """Transform safety incident record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if result is None:
        return None

    extra = decision.extra or {}

    if extra.get("incident_types"):
        result["incident_types"] = extra["incident_types"]

    if extra.get("severity"):
        result["severity"] = extra["severity"]

    if extra.get("compliance_score"):
        result["compliance_score"] = extra["compliance_score"]

    if extra.get("has_pii"):
        result["_pii_detected"] = True

    return result


__all__ = ["filter_record", "transform_record", "classify_incident_type", "assess_severity"]
