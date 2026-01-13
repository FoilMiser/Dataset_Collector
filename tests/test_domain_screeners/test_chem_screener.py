from __future__ import annotations

from collector_core.yellow.domains import chem


def test_chem_allows_legitimate_research(domain_ctx) -> None:
    raw = {
        "text": (
            "This peer-reviewed study analyzes the crystallographic structure of "
            "compound 7732-18-5 using X-ray diffraction and NMR spectroscopy. "
            "Safety data sheet (SDS) information is provided."
        ),
        "license": "CC-BY-4.0",
    }

    decision = chem.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert decision.extra["cas_numbers_found"] >= 1
    assert decision.extra["quality_score"] > 0


def test_chem_extracts_valid_cas_numbers(domain_ctx) -> None:
    raw = {
        "text": (
            "Water (7732-18-5) and ethanol (64-17-5) are common solvents. "
            "Invalid CAS: 1234-56-7."
        ),
        "license": "MIT",
    }

    decision = chem.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert decision.extra["cas_numbers_found"] == 3
    # 7732-18-5 and 64-17-5 are valid CAS numbers
    assert decision.extra["cas_numbers_valid"] >= 2


def test_chem_rejects_controlled_substance_synthesis(domain_ctx) -> None:
    raw = {
        "text": (
            "This document describes the synthesis route for methamphetamine "
            "using precursor chemicals."
        ),
        "license": "CC0-1.0",
    }

    decision = chem.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "controlled_substance_content"
    assert decision.extra is not None
    assert decision.extra["rejection_type"] == "dual_use"


def test_chem_rejects_chemical_weapons_content(domain_ctx) -> None:
    raw = {
        "text": "Information about nerve agent sarin and its effects.",
        "license": "CC0-1.0",
    }

    decision = chem.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "controlled_substance_content"


def test_chem_rejects_explosives_synthesis(domain_ctx) -> None:
    raw = {
        "text": "Synthesis of TATP requires certain precursors.",
        "license": "CC0-1.0",
    }

    decision = chem.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "controlled_substance_content"


def test_chem_transform_adds_cas_numbers(domain_ctx) -> None:
    raw = {
        "text": "Water (7732-18-5) is essential for life. Peer-reviewed research.",
        "license": "MIT",
    }

    decision = chem.filter_record(raw, domain_ctx)
    result = chem.transform_record(raw, domain_ctx, decision, license_profile="permissive")

    assert result is not None
    assert "screening" in result
    assert result["screening"]["domain"] == "chem"
    assert "extracted_cas_numbers" in result
    assert "7732-18-5" in result["extracted_cas_numbers"]


def test_validate_cas_number_valid() -> None:
    assert chem.validate_cas_number("7732-18-5") is True  # Water
    assert chem.validate_cas_number("64-17-5") is True  # Ethanol


def test_validate_cas_number_invalid() -> None:
    assert chem.validate_cas_number("1234-56-7") is False
    assert chem.validate_cas_number("invalid") is False


def test_extract_cas_numbers() -> None:
    text = "Water (7732-18-5) and ethanol (64-17-5) are solvents."
    result = chem.extract_cas_numbers(text)

    assert len(result) == 2
    cas_numbers = [r["cas_number"] for r in result]
    assert "7732-18-5" in cas_numbers
    assert "64-17-5" in cas_numbers
