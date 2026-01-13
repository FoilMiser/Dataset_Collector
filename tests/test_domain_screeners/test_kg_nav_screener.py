from __future__ import annotations

from collector_core.yellow.domains import kg_nav


def test_kg_nav_allows_relation_record(domain_ctx) -> None:
    raw = {
        "text": "Paris located_in France with coordinates latitude 48.8566 longitude 2.3522.",
        "license": "CC0-1.0",
    }

    decision = kg_nav.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert decision.extra["quality"]["signals"]["entities"] >= 2

    transformed = kg_nav.transform_record(raw, domain_ctx, decision, license_profile="permissive")
    assert transformed is not None
    assert transformed["screening"]["domain"] == "kg_nav"


def test_kg_nav_rejects_missing_relation(domain_ctx) -> None:
    raw = {
        "text": "Berlin Germany entity list without relation.",
        "license": "CC0-1.0",
    }

    decision = kg_nav.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "relation_missing"
