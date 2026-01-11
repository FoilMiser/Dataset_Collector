#!/usr/bin/env python3
"""
acquire_worker.py (v2.0)

Thin wrapper that delegates to the spec-driven generic acquire worker.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.acquire_strategies import (  # noqa: E402
    AcquireContext,
    Limits,
    RetryConfig,
    Roots,
    RunMode,
    _http_download_with_resume,
)
from collector_core.generic_workers import main_acquire  # noqa: E402

DOMAIN = "materials_science"

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
    main_acquire(DOMAIN, repo_root=Path(__file__).resolve().parents[1])


if __name__ == "__main__":
    main()
