#!/usr/bin/env python3
"""PMC worker wrapper for the engineering pipeline."""

from __future__ import annotations
from collector_core.pmc_worker import run_pmc_worker  # noqa: E402

if __name__ == "__main__":
    run_pmc_worker(
        pipeline_id="engineering",
        pools_root_default="/data/engineering/pools",
        log_dir_default="/data/engineering/_logs",
    )
