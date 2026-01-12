#!/usr/bin/env python3
"""
merge_worker.py (v2.0)

Deprecated compatibility shim for `dc run --pipeline safety_incident --stage merge`.
Removal target: v4.0.
"""
from __future__ import annotations

import warnings

from collector_core.generic_workers import main_merge  # noqa: E402

DOMAIN = "safety_incident"
DEPRECATION_MESSAGE = (
    "merge_worker.py is deprecated; use `dc run --pipeline safety_incident --stage merge` instead. "
    "Removal target: v4.0."
)


def main() -> None:
    warnings.warn(DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    main_merge(DOMAIN)


if __name__ == "__main__":
    main()
