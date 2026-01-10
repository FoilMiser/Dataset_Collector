"""
collector_core/generic_workers.py

Generic worker implementations that can be parameterized by pipeline spec.
Replaces per-pipeline acquire_worker.py, merge_worker.py, etc.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from collector_core.acquire_strategies import (
    DEFAULT_STRATEGY_HANDLERS,
    RootsDefaults,
    run_acquire_worker,
)
from collector_core.pipeline_spec import PipelineSpec, get_pipeline_spec

if TYPE_CHECKING:
    pass


def run_acquire_for_pipeline(spec: PipelineSpec) -> None:
    """Run the acquire worker for a pipeline specification."""
    roots = spec.get_default_roots()
    defaults = RootsDefaults(
        raw_root=roots["raw_root"],
        manifests_root=roots["manifests_root"],
        ledger_root=roots["ledger_root"],
        logs_root=roots["logs_root"],
    )
    run_acquire_worker(
        defaults=defaults,
        targets_yaml_label=spec.targets_yaml,
        strategy_handlers=DEFAULT_STRATEGY_HANDLERS,
    )


def main_acquire(domain: str) -> None:
    """Entry point for acquire worker."""
    spec = get_pipeline_spec(domain)
    if spec is None:
        print(f"Unknown pipeline domain: {domain}", file=sys.stderr)
        sys.exit(1)
    run_acquire_for_pipeline(spec)
