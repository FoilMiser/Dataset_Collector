#!/usr/bin/env python3
"""Deprecated pipeline entry point for catalog builder."""

from pathlib import Path

from collector_core.pipeline_cli import run_deprecated_entrypoint

if __name__ == "__main__":
    raise SystemExit(
        run_deprecated_entrypoint(
            "catalog-builder",
            pipeline_id=Path(__file__).resolve().parent.name,
        )
    )
