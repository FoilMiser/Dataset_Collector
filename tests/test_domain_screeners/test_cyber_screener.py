from __future__ import annotations

from collector_core.yellow.domains import cyber


def test_cyber_allows_research_with_cve(domain_ctx) -> None:
    raw = {
        "text": (
            "This advisory analyzes CVE-2023-1234 with mitigation guidance and a report "
            "on impacted systems."
        ),
        "license": "CC-BY-4.0",
    }

    decision = cyber.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert decision.extra["quality"]["signals"]["cve_count"] == 1

    transformed = cyber.transform_record(raw, domain_ctx, decision, license_profile="permissive")
    assert transformed is not None
    assert transformed["screening"]["domain"] == "cyber"


def test_cyber_rejects_missing_cve(domain_ctx) -> None:
    raw = {
        "text": "Exploit details without a CVE identifier should be rejected.",
        "license": "CC0-1.0",
    }

    decision = cyber.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "cve_missing"
