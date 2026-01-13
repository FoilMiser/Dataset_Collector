"""NLP-specific yellow screening with language and content detection.

This module provides NLP-specific filtering including:
- Language detection
- Toxicity/hate speech pattern detection
- PII detection (names, emails, phone numbers)
- Quality assessment (vocabulary diversity, coherence indicators)
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

# PII patterns
PII_PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "phone_us": re.compile(r"\b(?:\+1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}\b"),
    "phone_intl": re.compile(r"\b\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}\b"),
    "ssn": re.compile(r"\b\d{3}[-.\s]?\d{2}[-.\s]?\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d{4}[-.\s]?){3}\d{4}\b"),
    "ip_address": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
}

# Toxicity/hate speech patterns (simplified detection)
TOXICITY_PATTERNS = [
    re.compile(r"\b(kill|murder|attack)\s+(all|every|the)\s+\w+s?\b", re.IGNORECASE),
    re.compile(r"\b(hate|destroy|eliminate)\s+(all|every)\s+\w+\b", re.IGNORECASE),
    re.compile(r"\bdeath\s+to\s+\w+\b", re.IGNORECASE),
    re.compile(r"\b(inferior|subhuman)\s+(race|people|group)\b", re.IGNORECASE),
]

# Quality indicators for NLP content
QUALITY_INDICATORS = [
    "peer-reviewed",
    "published",
    "journal",
    "conference",
    "proceedings",
    "abstract",
    "methodology",
    "results",
    "conclusion",
    "references",
    "citation",
    "doi:",
]

# Language detection heuristics (common words by language)
LANGUAGE_HINTS: dict[str, list[str]] = {
    "en": ["the", "and", "is", "are", "was", "were", "have", "has", "this", "that"],
    "es": ["el", "la", "los", "las", "es", "son", "está", "están", "que", "de"],
    "fr": ["le", "la", "les", "est", "sont", "que", "qui", "dans", "avec", "pour"],
    "de": ["der", "die", "das", "ist", "sind", "und", "mit", "von", "für", "nicht"],
    "zh": ["的", "是", "在", "有", "和", "了", "不", "这", "为", "我"],
    "ja": ["の", "は", "が", "を", "に", "で", "と", "も", "た", "です"],
}


def detect_pii(text: str) -> dict[str, int]:
    """Detect PII patterns in text."""
    results = {}
    for pii_type, pattern in PII_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            results[pii_type] = len(matches)
    return results


def detect_toxicity(text: str) -> tuple[bool, str | None]:
    """Detect toxic/hate speech patterns."""
    for pattern in TOXICITY_PATTERNS:
        match = pattern.search(text)
        if match:
            return True, match.group(0)[:50]
    return False, None


def estimate_language(text: str) -> tuple[str, float]:
    """Estimate primary language using word frequency heuristics."""
    text_lower = text.lower()
    words = set(re.findall(r"\b\w+\b", text_lower))

    best_lang = "unknown"
    best_score = 0.0

    for lang, hints in LANGUAGE_HINTS.items():
        score = sum(1 for hint in hints if hint in words) / len(hints)
        if score > best_score:
            best_score = score
            best_lang = lang

    return best_lang, best_score


def compute_quality_score(text: str) -> tuple[int, list[str]]:
    """Compute quality score based on indicators."""
    text_lower = text.lower()
    matched = [ind for ind in QUALITY_INDICATORS if ind in text_lower]
    return len(matched), matched


def compute_vocabulary_diversity(text: str) -> float:
    """Compute type-token ratio as vocabulary diversity metric."""
    words = re.findall(r"\b\w+\b", text.lower())
    if not words:
        return 0.0
    unique_words = set(words)
    return len(unique_words) / len(words)


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """NLP-specific filtering with content analysis."""
    text = raw.get("text", "") or raw.get("content", "") or ""

    # Check for toxicity first (hard reject)
    has_toxicity, toxicity_match = detect_toxicity(text)
    if has_toxicity:
        return FilterDecision(
            allow=False,
            reason="toxic_content",
            text=text[:500] if text else None,
            extra={"rejection_type": "toxicity", "matched_content": toxicity_match},
        )

    # Detect PII
    pii_found = detect_pii(text)

    # Estimate language
    language, lang_confidence = estimate_language(text)

    # Quality assessment
    quality_score, quality_matches = compute_quality_score(text)
    vocab_diversity = compute_vocabulary_diversity(text)

    # Run standard filter
    decision = standard_filter(raw, ctx)

    # Add NLP-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update(
        {
            "detected_language": language,
            "language_confidence": lang_confidence,
            "pii_detected": pii_found,
            "pii_count": sum(pii_found.values()),
            "quality_score": quality_score,
            "quality_indicators": quality_matches,
            "vocabulary_diversity": vocab_diversity,
        }
    )

    # Flag if PII detected (but don't reject automatically)
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
    """Transform NLP record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if result is None:
        return None

    extra = decision.extra or {}

    if extra.get("detected_language"):
        result["detected_language"] = extra["detected_language"]
        result["language_confidence"] = extra.get("language_confidence", 0.0)

    if extra.get("vocabulary_diversity"):
        result["vocabulary_diversity"] = extra["vocabulary_diversity"]

    if extra.get("has_pii"):
        result["_pii_detected"] = True

    return result


__all__ = ["filter_record", "transform_record", "detect_pii", "detect_toxicity"]
