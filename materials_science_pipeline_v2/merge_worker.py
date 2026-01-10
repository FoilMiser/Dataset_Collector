#!/usr/bin/env python3
"""
merge_worker.py (v2.0)

Thin adapter for collector_core.merge.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core import merge as core_merge

PIPELINE_ID = Path(__file__).resolve().parent.name
DEFAULT_ROOTS = core_merge.default_merge_roots("materials")

read_jsonl = core_merge.read_jsonl
write_json = core_merge.write_json


def resolve_roots(cfg: dict) -> core_merge.Roots:
    return core_merge.resolve_roots(cfg, DEFAULT_ROOTS)


def merge_records(cfg: dict, roots: core_merge.Roots, execute: bool) -> dict:
    return core_merge.merge_records(cfg, roots, execute, pipeline_id=PIPELINE_ID)


def main() -> None:
    core_merge.main(pipeline_id=PIPELINE_ID, defaults=DEFAULT_ROOTS)


if __name__ == "__main__":
    main()
