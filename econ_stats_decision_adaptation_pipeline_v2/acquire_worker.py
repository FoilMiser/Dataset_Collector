#!/usr/bin/env python3
"""
acquire_worker.py (v2.0)

Deprecated compatibility shim for `dc run --pipeline econ_stats_decision_adaptation --stage acquire`.
Removal target: v3.0.
"""
from __future__ import annotations

import warnings
from pathlib import Path

from collector_core.acquire_strategies import (  # noqa: E402
    AcquireContext,
    Limits,
    RetryConfig,
    Roots,
    RunMode,
    _http_download_with_resume,
)
from collector_core.generic_workers import main_acquire  # noqa: E402

DOMAIN = "econ_stats_decision_adaptation"
DEPRECATION_MESSAGE = (
    "acquire_worker.py is deprecated; use `dc run --pipeline econ_stats_decision_adaptation --stage acquire` instead. "
    "Removal target: v3.0."
)

__all__ = [
    "AcquireContext",
    "Limits",
    "RetryConfig",
    "Roots",
    "RunMode",
    "_http_download_with_resume",
    "main",
]


def main() -> None:
    warnings.warn(DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    main_acquire(DOMAIN, repo_root=Path(__file__).resolve().parents[1])


if __name__ == "__main__":
    main()
