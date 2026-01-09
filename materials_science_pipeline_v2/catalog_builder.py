#!/usr/bin/env python3
"""Deprecated pipeline entry point for catalog builder."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.pipeline_cli import run_deprecated_entrypoint

if __name__ == "__main__":
    raise SystemExit(
        run_deprecated_entrypoint(
            "catalog-builder",
            pipeline_id=Path(__file__).resolve().parent.name,
        )
    )
