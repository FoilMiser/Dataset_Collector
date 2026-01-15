from __future__ import annotations

import re
from dataclasses import dataclass

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)

_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_PHONE_RE = re.compile(r"\b(?:\+?\d{1,3}[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}\b")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")

_STOPWORDS = {
    "the",
    "and",
    "is",
    "in",
    "to",
    "of",
    "for",
    "with",
    "that",
    "this",
    "on",
    "as",
    "by",
    "from",
}
_TOXIC_TERMS = {
    "hate",
    "kill",
    "racist",
    "bigot",
    "slur",
    "violence",
}
_LANG_ALLOWLIST = {"en", "eng", "english"}


@dataclass(frozen=True)
class _NlpSignals:
    language: str
    language_ok: bool
    toxicity_hits: int
    pii_detected: bool
    length_score: float


def _extract_language(raw: dict, text: str) -> str:
    for key in ("language", "lang", "locale"):
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    tokens = re.findall(r"[a-zA-Z]+", text.lower())
    stopword_hits = sum(1 for token in tokens if token in _STOPWORDS)
    return "en" if stopword_hits >= 2 else "unknown"


def _count_toxicity(text: str) -> int:
    tokens = set(re.findall(r"[a-zA-Z]+", text.lower()))
    return sum(1 for term in _TOXIC_TERMS if term in tokens)


def _contains_pii(text: str) -> bool:
    return bool(_EMAIL_RE.search(text) or _PHONE_RE.search(text) or _SSN_RE.search(text))


def _score_quality(signals: _NlpSignals) -> float:
    clean_score = 1.0 if signals.toxicity_hits == 0 and not signals.pii_detected else 0.3
    language_score = 1.0 if signals.language_ok else 0.2
    score = 0.45 * signals.length_score + 0.35 * language_score + 0.2 * clean_score
    return max(0.0, min(1.0, round(score, 3)))


def filter_record(raw: dict, ctx: DomainContext) -> FilterDecision:
    decision = standard_filter(raw, ctx)
    if not decision.allow or not decision.text:
        return decision

    language = _extract_language(raw, decision.text)
    language_ok = language in _LANG_ALLOWLIST
    toxicity_hits = _count_toxicity(decision.text)
    pii_detected = _contains_pii(decision.text)
    length_score = min(len(decision.text) / 800, 1.0)
    signals = _NlpSignals(
        language=language,
        language_ok=language_ok,
        toxicity_hits=toxicity_hits,
        pii_detected=pii_detected,
        length_score=length_score,
    )

    if not language_ok:
        return FilterDecision(
            allow=False,
            reason="language_not_allowed",
            text=decision.text,
            license_spdx=decision.license_spdx,
            extra={"language": language},
            sample_extra={"language": language},
        )
    if toxicity_hits:
        return FilterDecision(
            allow=False,
            reason="toxicity_detected",
            text=decision.text,
            license_spdx=decision.license_spdx,
            extra={"toxicity_hits": toxicity_hits},
            sample_extra={"toxicity_hits": toxicity_hits},
        )
    if pii_detected:
        return FilterDecision(
            allow=False,
            reason="pii_detected",
            text=decision.text,
            license_spdx=decision.license_spdx,
            extra={"pii_detected": True},
            sample_extra={"pii_detected": True},
        )

    quality_score = _score_quality(signals)
    extra = {
        "quality": {
            "score": quality_score,
            "signals": {
                "language": language,
                "toxicity_hits": toxicity_hits,
                "pii_detected": pii_detected,
                "length_score": round(length_score, 3),
            },
        }
    }
    return FilterDecision(
        allow=True,
        text=decision.text,
        license_spdx=decision.license_spdx,
        extra=extra,
    )


def transform_record(
    raw: dict,
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict | None:
    record = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if record is None:
        return None
    screening = {"domain": "nlp"}
    if decision.extra:
        screening.update(decision.extra)
    record["screening"] = screening
    return record


__all__ = ["filter_record", "transform_record"]
