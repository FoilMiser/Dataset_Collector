"""Knowledge graph and navigation-specific yellow screening.

This module provides KG/navigation-specific filtering including:
- Entity validation patterns
- Relationship extraction quality assessment
- Geospatial data validation
- Ontology compliance checks
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

# Entity ID patterns from common knowledge graphs
ENTITY_ID_PATTERNS = {
    "wikidata": re.compile(r"\b(Q\d{1,10})\b"),
    "dbpedia": re.compile(r"dbpedia\.org/resource/([A-Za-z0-9_]+)"),
    "freebase": re.compile(r"\b(/m/[a-z0-9_]+)\b"),
    "schema_org": re.compile(r"schema\.org/([A-Za-z]+)"),
    "owl": re.compile(r"#([A-Za-z][A-Za-z0-9_]*)"),
}

# Geospatial coordinate patterns
GEO_PATTERNS = {
    "lat_long": re.compile(
        r"[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)[,\s]+[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)"
    ),
    "dms": re.compile(
        r"\d{1,3}°\s*\d{1,2}['′]\s*\d{1,2}(?:\.\d+)?[\"″]?\s*[NSEW]"
    ),
    "geohash": re.compile(r"\b[0-9bcdefghjkmnpqrstuvwxyz]{5,12}\b"),
    "plus_code": re.compile(r"\b[23456789CFGHJMPQRVWX]{4,8}\+[23456789CFGHJMPQRVWX]{2,}\b"),
}

# Relationship/predicate patterns
RELATIONSHIP_PATTERNS = [
    re.compile(r"\b(is[_\s]?a|type[_\s]?of|instance[_\s]?of)\b", re.IGNORECASE),
    re.compile(r"\b(part[_\s]?of|belongs[_\s]?to|member[_\s]?of)\b", re.IGNORECASE),
    re.compile(r"\b(located[_\s]?in|located[_\s]?at|has[_\s]?location)\b", re.IGNORECASE),
    re.compile(r"\b(connected[_\s]?to|linked[_\s]?to|related[_\s]?to)\b", re.IGNORECASE),
    re.compile(r"\b(has[_\s]?property|has[_\s]?attribute|has[_\s]?value)\b", re.IGNORECASE),
]

# Ontology/schema indicators
ONTOLOGY_INDICATORS = [
    "owl:",
    "rdf:",
    "rdfs:",
    "skos:",
    "foaf:",
    "dc:",
    "dcterms:",
    "@context",
    "@type",
    "@id",
    "ontology",
    "taxonomy",
    "schema",
    "vocabulary",
]

# Data quality indicators for KG
QUALITY_INDICATORS = [
    "verified",
    "validated",
    "curated",
    "authoritative",
    "official",
    "canonical",
    "normalized",
    "disambiguated",
    "linked data",
    "semantic web",
]


def extract_entity_ids(text: str) -> dict[str, list[str]]:
    """Extract entity IDs from various knowledge graphs."""
    results: dict[str, list[str]] = {}
    for kg_name, pattern in ENTITY_ID_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            # Handle tuple results from groups
            if matches and isinstance(matches[0], tuple):
                matches = [m[0] if isinstance(m, tuple) else m for m in matches]
            results[kg_name] = list(set(matches))[:20]  # Dedupe and limit
    return results


def extract_geo_coordinates(text: str) -> dict[str, Any]:
    """Extract and validate geospatial coordinates."""
    lat_longs = GEO_PATTERNS["lat_long"].findall(text)
    dms_coords = GEO_PATTERNS["dms"].findall(text)

    return {
        "decimal_coordinates": len(lat_longs),
        "dms_coordinates": len(dms_coords),
        "has_geo_data": len(lat_longs) > 0 or len(dms_coords) > 0,
    }


def assess_relationship_quality(text: str) -> tuple[int, list[str]]:
    """Assess quality of relationship extraction."""
    matched_types = []
    for pattern in RELATIONSHIP_PATTERNS:
        matches = pattern.findall(text)
        if matches:
            matched_types.extend(matches[:5])  # Limit per type
    return len(matched_types), list(set(matched_types))


def check_ontology_compliance(text: str) -> tuple[int, list[str]]:
    """Check for ontology/schema compliance indicators."""
    text_lower = text.lower()
    matched = [ind for ind in ONTOLOGY_INDICATORS if ind.lower() in text_lower]
    return len(matched), matched


def check_quality_indicators(text: str) -> tuple[int, list[str]]:
    """Check for KG data quality indicators."""
    text_lower = text.lower()
    matched = [ind for ind in QUALITY_INDICATORS if ind in text_lower]
    return len(matched), matched


def filter_record(raw: dict[str, Any], ctx: DomainContext) -> FilterDecision:
    """Knowledge graph/navigation-specific filtering."""
    text = (
        raw.get("text", "")
        or raw.get("content", "")
        or raw.get("description", "")
        or raw.get("abstract", "")
        or ""
    )

    # Extract entity IDs
    entity_ids = extract_entity_ids(text)
    total_entities = sum(len(ids) for ids in entity_ids.values())

    # Extract geo coordinates
    geo_info = extract_geo_coordinates(text)

    # Assess relationship quality
    rel_score, rel_types = assess_relationship_quality(text)

    # Check ontology compliance
    ontology_score, ontology_matches = check_ontology_compliance(text)

    # Check quality indicators
    quality_score, quality_matches = check_quality_indicators(text)

    # Run standard filter
    decision = standard_filter(raw, ctx)

    # Add KG-specific metadata
    decision.extra = decision.extra or {}
    decision.extra.update(
        {
            "entity_ids": entity_ids,
            "total_entities_found": total_entities,
            "geo_info": geo_info,
            "relationship_score": rel_score,
            "relationship_types": rel_types,
            "ontology_score": ontology_score,
            "ontology_indicators": ontology_matches,
            "quality_score": quality_score,
            "quality_indicators": quality_matches,
        }
    )

    # Flag high-quality KG content
    if ontology_score >= 3 or total_entities >= 5:
        decision.extra["kg_rich_content"] = True

    return decision


def transform_record(
    raw: dict[str, Any],
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict[str, Any] | None:
    """Transform KG/navigation record with domain-specific fields."""
    result = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if result is None:
        return None

    extra = decision.extra or {}

    if extra.get("entity_ids"):
        result["extracted_entity_ids"] = extra["entity_ids"]

    if extra.get("geo_info", {}).get("has_geo_data"):
        result["has_geo_data"] = True

    if extra.get("ontology_score"):
        result["ontology_compliance_score"] = extra["ontology_score"]

    if extra.get("kg_rich_content"):
        result["kg_rich_content"] = True

    return result


__all__ = ["filter_record", "transform_record", "extract_entity_ids", "extract_geo_coordinates"]
