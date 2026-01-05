#!/usr/bin/env python3
"""PMC worker wrapper for the cyber pipeline."""

from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from collector_core.pmc_worker import run_pmc_worker


if __name__ == "__main__":
    run_pmc_worker(
        pipeline_id="cyber",
        pools_root_default="/data/cyber/pools",
        log_dir_default="/data/cyber/_logs",
    )
