from __future__ import annotations

from collector_core.yellow.domains import standard


def test_standard_rejects_missing_text(domain_ctx) -> None:
    decision = standard.filter_record({}, domain_ctx)
    assert decision.allow is False
    assert decision.reason == "no_text"


def test_standard_allows_and_transforms_record(domain_ctx) -> None:
    raw = {"text": "Sample text for screening.", "license": "CC-BY-4.0"}
    decision = standard.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.text == raw["text"]

    transformed = standard.transform_record(raw, domain_ctx, decision, license_profile="permissive")
    assert transformed is not None
    assert transformed["text"] == raw["text"]
    assert transformed["source"]["target_id"] == domain_ctx.target_id
    assert "content_sha256" in transformed["hash"]
