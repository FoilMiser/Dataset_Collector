#!/usr/bin/env python3
"""
pipeline_driver.py (v2.0)

Thin wrapper that delegates to the spec-driven pipeline factory.
"""
from __future__ import annotations
from pathlib import Path
from collector_core.pipeline_factory import get_pipeline_driver  # noqa: E402

DOMAIN = "biology"

if __name__ == "__main__":
    get_pipeline_driver(DOMAIN).main()
