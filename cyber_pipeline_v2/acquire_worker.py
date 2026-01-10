#!/usr/bin/env python3
"""
acquire_worker.py (v2.0)

Thin wrapper that delegates to the spec-driven generic acquire worker.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.generic_workers import main_acquire  # noqa: E402

DOMAIN = "cyber"

if __name__ == "__main__":
    main_acquire(DOMAIN)
