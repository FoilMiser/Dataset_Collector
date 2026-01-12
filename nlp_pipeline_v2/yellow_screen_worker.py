#!/usr/bin/env python3
"""
yellow_screen_worker.py (v2.0)

Deprecated compatibility shim for `dc run --pipeline nlp --stage yellow_screen`.
Removal target: v4.0.
"""
from __future__ import annotations

import warnings

from collector_core.yellow_screen_dispatch import main_yellow_screen  # noqa: E402

DOMAIN = "nlp"
DEPRECATION_MESSAGE = (
    "yellow_screen_worker.py is deprecated; use `dc run --pipeline nlp --stage yellow_screen` instead. "
    "Removal target: v4.0."
)

if __name__ == "__main__":
    warnings.warn(DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    main_yellow_screen(DOMAIN)
