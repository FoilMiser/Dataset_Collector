from __future__ import annotations

from collector_core.yellow.base import (
    DomainContext,
    FilterDecision,
    standard_filter,
    standard_transform,
)


def filter_record(raw: dict, ctx: DomainContext) -> FilterDecision:
    return standard_filter(raw, ctx)


def transform_record(
    raw: dict,
    ctx: DomainContext,
    decision: FilterDecision,
    *,
    license_profile: str,
) -> dict | None:
    return standard_transform(raw, ctx, decision, license_profile=license_profile)


__all__ = ["filter_record", "transform_record"]
