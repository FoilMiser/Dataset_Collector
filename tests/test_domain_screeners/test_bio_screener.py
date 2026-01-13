from __future__ import annotations

from collector_core.yellow.domains import biology


def test_biology_allows_legitimate_research(domain_ctx) -> None:
    raw = {
        "text": (
            "This peer-reviewed study published in Nature examines gene expression "
            "patterns using NCBI GeneID:7157 (TP53). Experimental validation was "
            "performed with clinical trial data."
        ),
        "license": "CC-BY-4.0",
    }

    decision = biology.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    assert decision.extra["quality_score"] > 0
    assert decision.extra["total_identifiers_found"] >= 1


def test_biology_extracts_gene_ids(domain_ctx) -> None:
    raw = {
        "text": (
            "Analysis of GeneID:7157 and ENSG00000141510 reveals regulatory patterns. "
            "UniProt accession P04637 corresponds to the tumor suppressor protein."
        ),
        "license": "MIT",
    }

    decision = biology.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    gene_ids = decision.extra.get("gene_ids", {})
    assert "ncbi_gene" in gene_ids or "ensembl" in gene_ids


def test_biology_detects_dna_sequences(domain_ctx) -> None:
    raw = {
        "text": (
            "The primer sequence is ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG. "
            "Published in peer reviewed journal."
        ),
        "license": "CC0-1.0",
    }

    decision = biology.filter_record(raw, domain_ctx)

    assert decision.allow is True
    assert decision.extra is not None
    seq_info = decision.extra.get("sequence_info", {})
    assert seq_info.get("has_dna_sequences") is True


def test_biology_rejects_biosecurity_content_select_agents(domain_ctx) -> None:
    raw = {
        "text": (
            "This document contains information about Bacillus anthracis (anthrax) "
            "cultivation methods."
        ),
        "license": "CC0-1.0",
    }

    decision = biology.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "biosecurity_concern"
    assert decision.extra is not None
    assert decision.extra["rejection_type"] == "biosecurity"


def test_biology_rejects_gain_of_function_research(domain_ctx) -> None:
    raw = {
        "text": (
            "This study describes gain-of-function experiments to enhance "
            "transmissibility of influenza virus."
        ),
        "license": "CC-BY-4.0",
    }

    decision = biology.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "biosecurity_concern"


def test_biology_rejects_pathogen_synthesis(domain_ctx) -> None:
    raw = {
        "text": "Instructions to synthesize a novel pathogen from scratch.",
        "license": "CC0-1.0",
    }

    decision = biology.filter_record(raw, domain_ctx)

    assert decision.allow is False
    assert decision.reason == "biosecurity_concern"


def test_biology_transform_adds_gene_ids(domain_ctx) -> None:
    raw = {
        "text": "Analysis of GeneID:7157 published in Nature.",
        "license": "MIT",
    }

    decision = biology.filter_record(raw, domain_ctx)
    result = biology.transform_record(raw, domain_ctx, decision, license_profile="permissive")

    assert result is not None
    assert "screening" in result
    assert result["screening"]["domain"] == "biology"


def test_biology_check_biosecurity_content() -> None:
    has_concern, match = biology.check_biosecurity_content(
        "Information about ebola virus transmission."
    )
    assert has_concern is True
    assert match is not None

    has_concern, match = biology.check_biosecurity_content(
        "Standard cell culture techniques for HeLa cells."
    )
    assert has_concern is False
    assert match is None


def test_biology_extract_gene_ids() -> None:
    text = "GeneID:7157 and UniProt P04637 are both TP53."
    result = biology.extract_gene_ids(text)

    assert "ncbi_gene" in result
    assert "7157" in result["ncbi_gene"]


def test_biology_detect_sequence_content() -> None:
    text = "The sequence is " + "ATCG" * 20 + " and protein is " + "ACDEFGHIKLMNPQRSTVWY" * 2
    result = biology.detect_sequence_content(text)

    assert result["has_dna_sequences"] is True
    assert result["dna_sequence_count"] >= 1
    assert result["has_protein_sequences"] is True
