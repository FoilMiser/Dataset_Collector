#!/usr/bin/env python3
"""
pipeline_driver.py (v2.0)

Thin wrapper that delegates to the spec-driven pipeline factory.
Includes custom build_row_extras for metrology-specific output fields.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.pipeline_factory import get_pipeline_driver  # noqa: E402

DOMAIN = "metrology"

_DriverBase = get_pipeline_driver(DOMAIN)


class MetrologyPipelineDriver(_DriverBase):  # type: ignore[valid-type,misc]
    """Metrology pipeline driver with custom row extras."""

    def build_row_extras(self, target: dict[str, Any], routing: dict[str, Any]) -> dict[str, Any]:
        mr = target.get("math_routing", {}) or {}
        return {
            "publisher": target.get("publisher"),
            "math_domain": mr.get("domain"),
            "math_category": mr.get("category"),
            "difficulty_level": mr.get("level"),
            "math_granularity": mr.get("granularity"),
        }


if __name__ == "__main__":
    MetrologyPipelineDriver.main()
