from __future__ import annotations

from collector_core.yellow.domains import econ


def test_econ_allows_financial_record(domain_ctx) -> None:
    raw = {
        "text": "Revenue guidance for 2023 shows improved cash flow and earnings momentum.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert 2023 in decision.extra["quality"]["signals"]["years_found"]

    transformed = econ.transform_record(raw, domain_ctx, decision, license_profile="permissive")
    assert transformed is not None
    assert transformed["screening"]["domain"] == "econ"


def test_econ_rejects_stale_timeframe(domain_ctx) -> None:
    raw = {
        "text": "The balance sheet from 2001 noted revenue declines and profit losses.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "stale_timeframe"
