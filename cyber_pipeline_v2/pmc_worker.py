#!/usr/bin/env python3
"""PMC worker wrapper for the cyber pipeline."""

from __future__ import annotations
from collector_core.pmc_worker import run_pmc_worker  # noqa: E402

if __name__ == "__main__":
    run_pmc_worker(
        pipeline_id="cyber",
        pools_root_default="/data/cyber/pools",
        log_dir_default="/data/cyber/_logs",
    )
