#!/usr/bin/env python3
"""
pipeline_driver.py (v2.0)

Thin wrapper that delegates to the spec-driven pipeline factory.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.pipeline_factory import get_pipeline_driver  # noqa: E402

DOMAIN = "materials_science"

if __name__ == "__main__":
    get_pipeline_driver(DOMAIN).main()
