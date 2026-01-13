"""Language detection content check."""

from __future__ import annotations

import re
from typing import Any

from collector_core.checks.implementations.base import CheckResult

check_name = "language_detect"

# Language detection heuristics (common words by language)
LANGUAGE_HINTS: dict[str, list[str]] = {
    "en": ["the", "and", "is", "are", "was", "were", "have", "has", "this", "that", "for", "with"],
    "es": ["el", "la", "los", "las", "es", "son", "está", "están", "que", "de", "por", "con"],
    "fr": ["le", "la", "les", "est", "sont", "que", "qui", "dans", "avec", "pour", "une", "des"],
    "de": ["der", "die", "das", "ist", "sind", "und", "mit", "von", "für", "nicht", "ein", "eine"],
    "zh": ["的", "是", "在", "有", "和", "了", "不", "这", "为", "我", "他", "她"],
    "ja": ["の", "は", "が", "を", "に", "で", "と", "も", "た", "です", "ます", "する"],
    "pt": ["o", "a", "os", "as", "é", "são", "que", "de", "para", "com", "em", "um"],
    "it": ["il", "la", "i", "le", "è", "sono", "che", "di", "per", "con", "un", "una"],
    "ru": ["и", "в", "не", "на", "что", "он", "она", "это", "как", "но", "с", "по"],
    "ar": ["في", "من", "على", "إلى", "أن", "هذا", "هذه", "التي", "الذي", "كان", "مع"],
}


def detect_language(text: str) -> tuple[str, float]:
    """Detect primary language using word frequency heuristics."""
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


def check(record: dict[str, Any], config: dict[str, Any]) -> CheckResult:
    """Run language detection check.
    
    Config options:
        required_languages: List of allowed language codes
        min_confidence: Minimum confidence for detection (default: 0.3)
        action_on_fail: Action when language not in allowed list (default: filter)
    """
    text = record.get("text", "") or record.get("content", "") or ""
    
    if not text:
        return CheckResult(passed=False, action="filter", reason="no_text_content")
    
    language, confidence = detect_language(text)
    
    required_languages = config.get("required_languages", [])
    min_confidence = config.get("min_confidence", 0.3)
    action_on_fail = config.get("action_on_fail", "filter")
    
    details = {
        "detected_language": language,
        "confidence": confidence,
    }
    
    if confidence < min_confidence:
        return CheckResult(
            passed=False,
            action="flag",
            reason="low_language_confidence",
            details=details,
            confidence=confidence,
        )
    
    if required_languages and language not in required_languages:
        return CheckResult(
            passed=False,
            action=action_on_fail,
            reason=f"language_not_allowed: {language}",
            details=details,
            confidence=confidence,
        )
    
    return CheckResult(passed=True, action="keep", details=details, confidence=confidence)


__all__ = ["check_name", "check", "detect_language"]
