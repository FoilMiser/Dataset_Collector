#!/usr/bin/env python3
"""PMC worker wrapper for the agri_circular pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if __package__ in (None, ""):
    sys.path.insert(0, str(REPO_ROOT))

from collector_core.pmc_worker import run_pmc_worker  # noqa: E402

if __name__ == "__main__":
    run_pmc_worker(
        pipeline_id="agri_circular",
        pools_root_default="/data/agri_circular/pools",
        log_dir_default="/data/agri_circular/_logs",
    )
