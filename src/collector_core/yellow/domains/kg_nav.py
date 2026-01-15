from __future__ import annotations

import re
from dataclasses import dataclass

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)

_ENTITY_RE = re.compile(r"\b[A-Z][a-zA-Z0-9_\-]{2,}\b")
_RELATION_TERMS = {"located_in", "part_of", "member_of", "capital_of", "adjacent_to"}
_GEO_TERMS = {"latitude", "longitude", "geo", "coordinates"}
_ONTOLOGY_TERMS = {"wikidata", "schema.org", "geonames", "ontology", "rdf", "owl"}


@dataclass(frozen=True)
class _KgSignals:
    entities: int
    relation_terms: int
    geo_terms: int
    ontology_terms: int
    length_score: float


def _score_quality(signals: _KgSignals) -> float:
    entity_score = 1.0 if signals.entities >= 2 else 0.5
    relation_score = 1.0 if signals.relation_terms else 0.6
    geo_score = 0.9 if signals.geo_terms else 0.6
    ontology_score = 0.9 if signals.ontology_terms else 0.6
    score = (
        0.35 * signals.length_score
        + 0.2 * entity_score
        + 0.2 * relation_score
        + 0.15 * geo_score
        + 0.1 * ontology_score
    )
    return max(0.0, min(1.0, round(score, 3)))


def filter_record(raw: dict, ctx: DomainContext) -> FilterDecision:
    decision = standard_filter(raw, ctx)
    if not decision.allow or not decision.text:
        return decision

    text = decision.text
    entities = len(_ENTITY_RE.findall(text))
    if entities < 2:
        return FilterDecision(
            allow=False,
            reason="entity_count_low",
            text=text,
            license_spdx=decision.license_spdx,
            extra={"entities": entities},
            sample_extra={"entities": entities},
        )

    lowered = text.lower()
    relation_terms = sum(1 for term in _RELATION_TERMS if term in lowered)
    if not relation_terms:
        return FilterDecision(
            allow=False,
            reason="relation_missing",
            text=text,
            license_spdx=decision.license_spdx,
            extra={"relation_terms": 0},
            sample_extra={"relation_terms": 0},
        )

    geo_terms = sum(1 for term in _GEO_TERMS if term in lowered)
    ontology_terms = sum(1 for term in _ONTOLOGY_TERMS if term in lowered)
    length_score = min(len(text) / 650, 1.0)
    signals = _KgSignals(
        entities=entities,
        relation_terms=relation_terms,
        geo_terms=geo_terms,
        ontology_terms=ontology_terms,
        length_score=length_score,
    )
    quality_score = _score_quality(signals)
    extra = {
        "quality": {
            "score": quality_score,
            "signals": {
                "entities": entities,
                "relation_terms": relation_terms,
                "geo_terms": geo_terms,
                "ontology_terms": ontology_terms,
                "length_score": round(length_score, 3),
            },
        }
    }
    return FilterDecision(
        allow=True,
        text=text,
        license_spdx=decision.license_spdx,
        extra=extra,
    )


def transform_record(
    raw: dict,
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict | None:
    record = standard_transform(raw, ctx, decision, license_profile=license_profile)
    if record is None:
        return None
    screening = {"domain": "kg_nav"}
    if decision.extra:
        screening.update(decision.extra)
    record["screening"] = screening
    return record


__all__ = ["filter_record", "transform_record"]
