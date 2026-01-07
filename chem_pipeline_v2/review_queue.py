#!/usr/bin/env python3
"""Deprecated pipeline entry point for manual YELLOW review queue helper."""

from pathlib import Path

from collector_core.pipeline_cli import run_deprecated_entrypoint

if __name__ == "__main__":
    raise SystemExit(
        run_deprecated_entrypoint(
            "review-queue",
            pipeline_id=Path(__file__).resolve().parent.name,
        )
    )
