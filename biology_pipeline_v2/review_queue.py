#!/usr/bin/env python3
"""
review_queue.py (v2.0)

Thin wrapper that delegates to the spec-driven review queue helper.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.generic_workers import main_review_queue  # noqa: E402

DOMAIN = "biology"


def main() -> None:
    main_review_queue(DOMAIN)


if __name__ == "__main__":
    main()
