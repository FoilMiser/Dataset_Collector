from __future__ import annotations

from collector_core.checks.implementations import toxicity_scan


def test_toxicity_scan_warns_on_matches() -> None:
    result = toxicity_scan.check(
        {"text": "This contains rude language."},
        {"toxicity_terms": ["rude", "violent"]},
    )
    assert result.check == toxicity_scan.check_name
    assert result.status == "warn"
    assert result.details["match_count"] == 1
    assert result.details["matches"] == {"rude": 1}


def test_toxicity_scan_skips_without_terms() -> None:
    result = toxicity_scan.check({"text": "clean"}, {})
    assert result.status == "skip"
    assert result.details["reason"] == "toxicity_terms_not_configured"
