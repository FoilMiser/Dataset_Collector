#!/usr/bin/env python3
"""
review_queue.py (v2.0)

Thin wrapper that delegates to the spec-driven review queue helper.
"""
from __future__ import annotations
from pathlib import Path
from collector_core.generic_workers import main_review_queue  # noqa: E402

DOMAIN = "agri_circular"


def main() -> None:
    main_review_queue(DOMAIN)


if __name__ == "__main__":
    main()
