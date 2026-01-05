#!/usr/bin/env python3
"""Pipeline entry point for manual YELLOW review queue helper."""

from pathlib import Path

from collector_core.review_queue import main as review_main

if __name__ == "__main__":
    review_main(pipeline_id=Path(__file__).resolve().parent.name)
