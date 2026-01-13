from __future__ import annotations

from collector_core.yellow.domains import nlp


def test_nlp_allows_english_record(domain_ctx) -> None:
    raw = {
        "text": "This study explores the impact of language models on modern AI systems.",
        "license": "CC-BY-4.0",
    }

    decision = nlp.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert decision.extra["quality"]["signals"]["language"] == "en"

    transformed = nlp.transform_record(raw, domain_ctx, decision, license_profile="permissive")
    assert transformed is not None
    assert transformed["screening"]["quality"]["score"] >= 0


def test_nlp_rejects_toxic_record(domain_ctx) -> None:
    raw = {
        "text": "This text includes hate speech and violence which should be rejected.",
        "license": "CC-BY-4.0",
    }

    decision = nlp.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "toxicity_detected"
