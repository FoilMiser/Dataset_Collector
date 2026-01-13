from __future__ import annotations

import re
from dataclasses import dataclass

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)


_INCIDENT_TERMS = {
    "incident",
    "accident",
    "near miss",
    "injury",
    "fatality",
    "spill",
    "fire",
    "evacuation",
    "hazard",
}
_SEVERITY_TERMS = {
    "fatal": "critical",
    "fatality": "critical",
    "death": "critical",
    "hospitalized": "high",
    "severe": "high",
    "injury": "medium",
    "minor": "low",
    "no injuries": "low",
}
_COMPLIANCE_TERMS = {
    "osha",
    "iso 45001",
    "incident report",
    "compliance",
    "regulator",
}
_PII_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


@dataclass(frozen=True)
class _SafetySignals:
    incident_terms: int
    severity: str
    pii_detected: bool
    compliance_hits: int
    length_score: float


def _detect_severity(text: str) -> str:
    lowered = text.lower()
    for term, label in _SEVERITY_TERMS.items():
        if term in lowered:
            return label
    return "unknown"


def _score_quality(signals: _SafetySignals) -> float:
    incident_score = 1.0 if signals.incident_terms else 0.4
    severity_score = {"critical": 1.0, "high": 0.9, "medium": 0.7, "low": 0.5}.get(
        signals.severity, 0.4
    )
    compliance_score = 0.9 if signals.compliance_hits else 0.6
    pii_score = 0.3 if signals.pii_detected else 1.0
    score = (
        0.35 * signals.length_score
        + 0.25 * incident_score
        + 0.2 * severity_score
        + 0.1 * compliance_score
        + 0.1 * pii_score
    )
    return max(0.0, min(1.0, round(score, 3)))


def filter_record(raw: dict, ctx: DomainContext) -> FilterDecision:
    decision = standard_filter(raw, ctx)
    if not decision.allow or not decision.text:
        return decision

    text = decision.text
    lowered = text.lower()
    incident_terms = sum(1 for term in _INCIDENT_TERMS if term in lowered)
    if not incident_terms:
        return FilterDecision(
            allow=False,
            reason="incident_terms_missing",
            text=text,
            license_spdx=decision.license_spdx,
            extra={"incident_terms": 0},
            sample_extra={"incident_terms": 0},
        )

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

    severity = _detect_severity(text)
    compliance_hits = sum(1 for term in _COMPLIANCE_TERMS if term in lowered)
    length_score = min(len(text) / 700, 1.0)
    signals = _SafetySignals(
        incident_terms=incident_terms,
        severity=severity,
        pii_detected=pii_detected,
        compliance_hits=compliance_hits,
        length_score=length_score,
    )
    quality_score = _score_quality(signals)
    extra = {
        "quality": {
            "score": quality_score,
            "signals": {
                "incident_terms": incident_terms,
                "severity": severity,
                "pii_detected": pii_detected,
                "compliance_hits": compliance_hits,
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
    screening = {"domain": "safety"}
    if decision.extra:
        screening.update(decision.extra)
    record["screening"] = screening
    return record


__all__ = ["filter_record", "transform_record"]
