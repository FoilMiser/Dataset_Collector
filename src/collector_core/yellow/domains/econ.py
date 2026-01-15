from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)

_FINANCIAL_TERMS = {
    "revenue",
    "earnings",
    "profit",
    "loss",
    "ebitda",
    "balance sheet",
    "cash flow",
    "forecast",
    "guidance",
    "valuation",
}
_SENSITIVE_TERMS = {"confidential", "non-public", "insider", "material", "mnpi"}
_METHODOLOGY_TERMS = {"methodology", "assumption", "regression", "sample", "survey", "model"}
_PII_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


@dataclass(frozen=True)
class _EconSignals:
    financial_terms: int
    sensitive_terms: int
    pii_detected: bool
    years_found: list[int]
    methodology_terms: int
    length_score: float


def _extract_years(text: str) -> list[int]:
    return [int(match.group(0)) for match in _YEAR_RE.finditer(text)]


def _score_quality(signals: _EconSignals) -> float:
    finance_score = 1.0 if signals.financial_terms else 0.5
    methodology_score = 1.0 if signals.methodology_terms else 0.6
    recency_score = 0.9 if signals.years_found else 0.6
    pii_score = 0.3 if signals.pii_detected else 1.0
    sensitivity_penalty = 0.4 if signals.sensitive_terms else 1.0
    score = (
        0.35 * signals.length_score
        + 0.2 * finance_score
        + 0.15 * methodology_score
        + 0.15 * recency_score
        + 0.1 * pii_score
        + 0.05 * sensitivity_penalty
    )
    return max(0.0, min(1.0, round(score, 3)))


def filter_record(raw: dict, ctx: DomainContext) -> FilterDecision:
    decision = standard_filter(raw, ctx)
    if not decision.allow or not decision.text:
        return decision

    text = decision.text
    lowered = text.lower()
    financial_terms = sum(1 for term in _FINANCIAL_TERMS if term in lowered)
    if not financial_terms:
        return FilterDecision(
            allow=False,
            reason="financial_terms_missing",
            text=text,
            license_spdx=decision.license_spdx,
            extra={"financial_terms": 0},
            sample_extra={"financial_terms": 0},
        )

    sensitive_terms = sum(1 for term in _SENSITIVE_TERMS if term in lowered)
    pii_detected = bool(_PII_RE.search(text))
    if pii_detected:
        return FilterDecision(
            allow=False,
            reason="pii_detected",
            text=text,
            license_spdx=decision.license_spdx,
            extra={"pii_detected": True},
            sample_extra={"pii_detected": True},
        )

    years_found = _extract_years(text)
    now_year = datetime.now(timezone.utc).year
    stale = all(year < now_year - 10 for year in years_found) if years_found else False
    if stale:
        return FilterDecision(
            allow=False,
            reason="stale_timeframe",
            text=text,
            license_spdx=decision.license_spdx,
            extra={"years_found": years_found},
            sample_extra={"years_found": years_found},
        )

    methodology_terms = sum(1 for term in _METHODOLOGY_TERMS if term in lowered)
    length_score = min(len(text) / 800, 1.0)
    signals = _EconSignals(
        financial_terms=financial_terms,
        sensitive_terms=sensitive_terms,
        pii_detected=pii_detected,
        years_found=years_found,
        methodology_terms=methodology_terms,
        length_score=length_score,
    )
    quality_score = _score_quality(signals)
    extra = {
        "quality": {
            "score": quality_score,
            "signals": {
                "financial_terms": financial_terms,
                "sensitive_terms": sensitive_terms,
                "pii_detected": pii_detected,
                "years_found": years_found,
                "methodology_terms": methodology_terms,
                "length_score": round(length_score, 3),
            },
        }
    }
    return FilterDecision(
        allow=True,
        text=text,
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
    screening = {"domain": "econ"}
    if decision.extra:
        screening.update(decision.extra)
    record["screening"] = screening
    return record


__all__ = ["filter_record", "transform_record"]
