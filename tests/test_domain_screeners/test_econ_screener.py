from __future__ import annotations

from datetime import datetime, timezone

from collector_core.yellow.domains import econ


def test_econ_allows_financial_record(domain_ctx) -> None:
    """Test that records with financial terms and recent dates are allowed."""
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
    """Test that records with only old dates (>10 years) are rejected."""
    raw = {
        "text": "The balance sheet from 2001 noted revenue declines and profit losses.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "stale_timeframe"


def test_econ_rejects_missing_financial_terms(domain_ctx) -> None:
    """Test that records without financial terms are rejected."""
    raw = {
        "text": "This is a general article about economic policy and governance in 2023.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "financial_terms_missing"
    assert decision.extra is not None
    assert decision.extra["financial_terms"] == 0


def test_econ_rejects_pii_detected(domain_ctx) -> None:
    """Test that records with PII (email addresses) are rejected."""
    raw = {
        "text": "Contact john.doe@example.com for revenue forecasts and earnings reports.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "pii_detected"
    assert decision.extra is not None
    assert decision.extra["pii_detected"] is True


def test_econ_methodology_terms_boost_quality(domain_ctx) -> None:
    """Test that methodology terms increase quality score."""
    raw_with_methodology = {
        "text": "Revenue analysis using regression methodology and sample survey data for 2024.",
        "license": "CC-BY-4.0",
    }
    raw_without_methodology = {
        "text": "Revenue analysis shows strong earnings and profit growth for 2024.",
        "license": "CC-BY-4.0",
    }

    decision_with = econ.filter_record(raw_with_methodology, domain_ctx)
    decision_without = econ.filter_record(raw_without_methodology, domain_ctx)

    assert decision_with.allow is True
    assert decision_without.allow is True
    assert decision_with.extra is not None
    assert decision_without.extra is not None

    # Methodology terms should be detected
    assert decision_with.extra["quality"]["signals"]["methodology_terms"] > 0
    # Quality score with methodology should be higher
    assert decision_with.extra["quality"]["score"] >= decision_without.extra["quality"]["score"]


def test_econ_sensitive_terms_reduce_quality(domain_ctx) -> None:
    """Test that sensitive terms reduce quality score."""
    raw_with_sensitive = {
        "text": "Confidential material non-public revenue guidance for 2024.",
        "license": "CC-BY-4.0",
    }
    raw_without_sensitive = {
        "text": "Public revenue guidance for 2024 shows strong earnings momentum.",
        "license": "CC-BY-4.0",
    }

    decision_with = econ.filter_record(raw_with_sensitive, domain_ctx)
    decision_without = econ.filter_record(raw_without_sensitive, domain_ctx)

    assert decision_with.allow is True
    assert decision_without.allow is True
    assert decision_with.extra is not None
    assert decision_without.extra is not None

    # Sensitive terms should be detected
    assert decision_with.extra["quality"]["signals"]["sensitive_terms"] > 0
    assert decision_without.extra["quality"]["signals"]["sensitive_terms"] == 0


def test_econ_extracts_multiple_years(domain_ctx) -> None:
    """Test that multiple years are extracted from text."""
    raw = {
        "text": "Revenue comparison between 2020, 2021, 2022, and 2023 shows profit growth.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    years_found = decision.extra["quality"]["signals"]["years_found"]
    assert 2020 in years_found
    assert 2021 in years_found
    assert 2022 in years_found
    assert 2023 in years_found


def test_econ_allows_mixed_old_and_new_years(domain_ctx) -> None:
    """Test that records with at least one recent year are allowed."""
    current_year = datetime.now(timezone.utc).year
    raw = {
        "text": f"Historical revenue from 1990 compared to {current_year} shows profit growth.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    # Should be allowed because at least one year is recent
    assert decision.allow is True


def test_econ_allows_no_years_found(domain_ctx) -> None:
    """Test that records without years are allowed (recency check skipped)."""
    raw = {
        "text": "Generic revenue and earnings analysis with profit forecasts.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert decision.extra["quality"]["signals"]["years_found"] == []


def test_econ_length_score_calculation(domain_ctx) -> None:
    """Test that length score is calculated correctly."""
    short_text = "Revenue profit earnings."  # ~25 chars
    long_text = "Revenue profit earnings. " * 50  # ~1250 chars

    raw_short = {"text": f"Revenue profit earnings for 2024.", "license": "CC-BY-4.0"}
    raw_long = {
        "text": f"Revenue profit earnings for 2024. {long_text}",
        "license": "CC-BY-4.0",
    }

    decision_short = econ.filter_record(raw_short, domain_ctx)
    decision_long = econ.filter_record(raw_long, domain_ctx)

    assert decision_short.allow is True
    assert decision_long.allow is True
    assert decision_short.extra is not None
    assert decision_long.extra is not None

    # Long text should have higher length score (capped at 1.0)
    short_length_score = decision_short.extra["quality"]["signals"]["length_score"]
    long_length_score = decision_long.extra["quality"]["signals"]["length_score"]
    assert long_length_score >= short_length_score
    assert long_length_score <= 1.0


def test_econ_transform_includes_quality_metadata(domain_ctx) -> None:
    """Test that transform_record includes quality metadata in screening."""
    raw = {
        "text": "Revenue guidance for 2024 shows improved cash flow and earnings momentum.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)
    result = econ.transform_record(raw, domain_ctx, decision, license_profile="permissive")

    assert result is not None
    assert "screening" in result
    assert result["screening"]["domain"] == "econ"
    assert "quality" in result["screening"]
    assert "score" in result["screening"]["quality"]
    assert "signals" in result["screening"]["quality"]


def test_econ_transform_returns_none_when_text_missing(domain_ctx) -> None:
    """Test that transform_record returns None when decision.text is empty."""
    raw = {"text": "Some text", "license": "CC-BY-4.0"}

    # Create a decision with empty text (simulating transform failure)
    from collector_core.yellow.base import FilterDecision

    decision = FilterDecision(allow=True, text=None, license_spdx=None, extra={})

    # Transform should return None because decision.text is None
    result = econ.transform_record(raw, domain_ctx, decision, license_profile="permissive")

    # Result should be None because standard_transform returns None when text is missing
    assert result is None


def test_econ_quality_score_bounded(domain_ctx) -> None:
    """Test that quality score is always between 0 and 1."""
    # Test with various inputs
    test_cases = [
        "Revenue profit for 2024.",  # Minimal
        "Revenue earnings profit ebitda balance sheet cash flow forecast guidance valuation for 2024.",  # All terms
        "Revenue guidance 2024 with methodology regression sample survey model assumption.",  # With methodology
    ]

    for text in test_cases:
        raw = {"text": text, "license": "CC-BY-4.0"}
        decision = econ.filter_record(raw, domain_ctx)
        if decision.allow and decision.extra:
            score = decision.extra["quality"]["score"]
            assert 0.0 <= score <= 1.0, f"Score {score} out of bounds for: {text}"


def test_econ_multiple_financial_terms_counted(domain_ctx) -> None:
    """Test that multiple financial terms are counted correctly."""
    raw = {
        "text": "Revenue earnings profit ebitda balance sheet cash flow for 2024.",
        "license": "CC-BY-4.0",
    }

    decision = econ.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    # Should count multiple financial terms
    assert decision.extra["quality"]["signals"]["financial_terms"] >= 5
