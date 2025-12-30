#!/usr/bin/env python3
"""CLI wrapper for running all Dataset Collector pipelines."""

from __future__ import annotations

import sys

from tools.build_natural_corpus import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
