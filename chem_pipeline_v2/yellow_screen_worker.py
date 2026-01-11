#!/usr/bin/env python3
"""
yellow_screen_worker.py (v2.0)

Thin wrapper that delegates to the spec-driven yellow screen dispatch.
"""
from __future__ import annotations
from collector_core.yellow_screen_dispatch import main_yellow_screen  # noqa: E402

DOMAIN = "chem"

if __name__ == "__main__":
    main_yellow_screen(DOMAIN)
