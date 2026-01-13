"""Biology-specific yellow screening with biosecurity checks.

This module provides biology-specific filtering including:
- Biosecurity screening for dangerous pathogen content
- Gene/protein ID extraction and validation
- Taxonomy verification
- Sequence data quality assessment
"""

from __future__ import annotations

import re
from typing import Any

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)

# Select Agent and Toxin patterns (CDC/USDA regulated)
BIOSECURITY_PATTERNS = [
    re.compile(
        r"\b(ebola|marburg|variola|smallpox|yersinia\s+pestis|"
        r"bacillus\s+anthracis|anthrax|botulinum|ricin|"
        r"francisella\s+tularensis|tularemia)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(gain[\s-]+of[\s-]+function|enhanced\s+transmissibility|"
        r"increased\s+virulence|pandemic\s+potential)\b"
        r".{0,100}"
        r"\b(influenza|coronavirus|sars|mers)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(synthesize|reconstruct|engineer|create)\b"
        r".{0,50}"
        r"\b(pathogen|virus|toxin|bioweapon)\b",
        re.IGNORECASE,
    ),
]

# Gene/Protein ID patterns
GENE_ID_PATTERNS = {
    "ncbi_gene": re.compile(r"\bGeneID[:\s]*(\d{4,10})\b", re.IGNORECASE),
    "ensembl": re.compile(r"\b(ENS[A-Z]{0,3}G\d{11})\b"),
    "uniprot": re.compile(r"\b([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})\b"),
    "refseq_mrna": re.compile(r"\b(NM_\d{6,9})\b"),
    "refseq_protein": re.compile(r"\b(NP_\d{6,9})\b"),
    "pdb": re.compile(r"\bPDB[:\s]*([0-9][A-Za-z0-9]{3})\b", re.IGNORECASE),
}

# Sequence patterns
DNA_SEQUENCE_PATTERN = re.compile(r"\b[ATCG]{50,}\b")
PROTEIN_SEQUENCE_PATTERN = re.compile(r"\b[ACDEFGHIKLMNPQRSTVWY]{30,}\b")

QUALITY_INDICATORS = [
    "peer-reviewed", "peer reviewed", "published in", "doi:", "pmid:",
    "pmc", "nature", "science", "cell", "experimental validation",
    "clinical trial", "ncbi", "genbank", "uniprot",
]


def check_biosecurity_content(text: str) -> tuple[bool, str | None]:
    """Check for biosecurity-sensitive content."""
    for pattern in BIOSECURITY_PATTERNS:
        match = pattern.search(text)
        if match:
            return True, match.group(0)[:100]
    return False, None


def extract_gene_ids(text: str) -> dict[str, list[str]]:
    """Extract gene and protein identifiers from text."""
    results: dict[str, list[str]] = {}
    for id_type, pattern in GENE_ID_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            if matches and isinstance(matches[0], tuple):
                matches = [m[0] for m in matches]
            results[id_type] = list(set(matches))[:20]
    return results


def detect_sequence_content(text: str) -> dict[str, Any]:
    """Detect and characterize sequence content."""
    dna_matches = DNA_SEQUENCE_PATTERN.findall(text)
    protein_matches = PROTEIN_SEQUENCE_PATTERN.findall(text)
    return {
        "has_dna_sequences": len(dna_matches) > 0,
        "dna_sequence_count": len(dna_matches),
        "total_dna_bases": sum(len(m) for m in dna_matches),
        "has_protein_sequences": len(protein_matches) > 0,
        "protein_sequence_count": len(protein_matches),
        "total_amino_acids": sum(len(m) for m in protein_matches),
    }


def compute_quality_score(text: str) -> tuple[int, list[str]]:
    """Compute quality score based on research indicators."""
    text_lower = text.lower()
    matched = [ind for ind in QUALITY_INDICATORS if ind in text_lower]
    return len(matched), matched


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Biology-specific filtering with biosecurity screening."""
    text = raw.get("text", "") or raw.get("abstract", "") or raw.get("content", "") or ""

    has_biosecurity, biosecurity_match = check_biosecurity_content(text)
    if has_biosecurity:
        return FilterDecision(
            allow=False, reason="biosecurity_concern",
            text=text[:500] if text else None,
            extra={"rejection_type": "biosecurity", "matched_content": biosecurity_match},
        )

    gene_ids = extract_gene_ids(text)
    sequence_info = detect_sequence_content(text)
    quality_score, quality_matches = compute_quality_score(text)

    decision = standard_filter(raw, ctx)
    decision.extra = decision.extra or {}
    decision.extra.update({
        "gene_ids": gene_ids,
        "sequence_info": sequence_info,
        "quality_score": quality_score,
        "quality_indicators": quality_matches,
        "total_identifiers_found": sum(len(ids) for ids in gene_ids.values()),
    })
    return decision


def transform_record(
    raw: dict[str, Any], ctx: DomainContext, decision: FilterDecision, *, license_profile: str,
) -> dict[str, Any] | None:
    """Transform biology record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if result is None:
        return None
    extra = decision.extra or {}
    if extra.get("gene_ids"):
        result["extracted_gene_ids"] = extra["gene_ids"]
    if extra.get("sequence_info"):
        result["sequence_statistics"] = extra["sequence_info"]
    return result


__all__ = ["filter_record", "transform_record"]
