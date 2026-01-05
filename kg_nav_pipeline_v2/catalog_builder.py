#!/usr/bin/env python3
"""Pipeline entry point for catalog builder."""

from pathlib import Path

from collector_core.catalog_builder import main as catalog_main

if __name__ == "__main__":
    catalog_main(pipeline_id=Path(__file__).resolve().parent.name)
