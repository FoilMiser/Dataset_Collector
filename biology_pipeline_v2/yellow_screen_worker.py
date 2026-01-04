#!/usr/bin/env python3
"""
yellow_screen_worker.py (v2.0)

Thin adapter for collector_core.yellow_screen_standard.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core import yellow_screen_standard as core_yellow
from collector_core.yellow_screen_common import default_yellow_roots

DEFAULT_ROOTS = default_yellow_roots("bio")


def resolve_roots(cfg: dict):
    return core_yellow.resolve_roots(cfg, DEFAULT_ROOTS)


def main() -> None:
    core_yellow.main(defaults=DEFAULT_ROOTS)


if __name__ == "__main__":
    main()
