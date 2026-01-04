#!/usr/bin/env python3
"""
yellow_screen_worker.py (v2.0)

Thin adapter for collector_core.yellow_screen_kg_nav.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.yellow_screen_kg_nav import main


if __name__ == "__main__":
    main()
