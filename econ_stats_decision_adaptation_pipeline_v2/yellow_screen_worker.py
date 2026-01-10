#!/usr/bin/env python3
"""
yellow_screen_worker.py (v2.0)

Thin adapter for collector_core.yellow_screen_econ.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.yellow_screen_econ import main

if __name__ == "__main__":
    main()
