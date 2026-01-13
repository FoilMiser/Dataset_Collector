from __future__ import annotations

from collector_core.yellow.domains import safety


def test_safety_allows_incident_report(domain_ctx) -> None:
    raw = {
        "text": "An accident occurred at the plant. OSHA guidelines were reviewed.",
        "license": "CC0-1.0",
    }

    decision = safety.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert decision.extra["quality"]["signals"]["severity"] in {"low", "unknown", "medium"}

    transformed = safety.transform_record(raw, domain_ctx, decision, license_profile="permissive")
    assert transformed is not None
    assert transformed["screening"]["domain"] == "safety"


def test_safety_rejects_pii(domain_ctx) -> None:
    raw = {
        "text": "Incident report contact: jane.doe@example.com for follow-up.",
        "license": "CC0-1.0",
    }

    decision = safety.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "pii_detected"
