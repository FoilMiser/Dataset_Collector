#!/usr/bin/env python3
"""
catalog_builder.py (v2.0)

Thin wrapper that delegates to the spec-driven generic catalog builder.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.generic_workers import main_catalog  # noqa: E402

DOMAIN = "safety_incident"


def main() -> None:
    main_catalog(DOMAIN)


if __name__ == "__main__":
    main()
