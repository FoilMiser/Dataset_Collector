#!/usr/bin/env python3
"""
merge_worker.py (v2.0)

Thin wrapper that delegates to the spec-driven generic merge worker.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core import merge as core_merge  # noqa: E402
from collector_core.generic_workers import main_merge  # noqa: E402
from collector_core.pipeline_spec import get_pipeline_spec  # noqa: E402

DOMAIN = "nlp"
SPEC = get_pipeline_spec(DOMAIN)
if SPEC is None:
    raise SystemExit(f"Unknown pipeline domain: {DOMAIN}")

PIPELINE_ID = SPEC.pipeline_id
DEFAULT_ROOTS = core_merge.default_merge_roots(SPEC.prefix)

read_jsonl = core_merge.read_jsonl
write_json = core_merge.write_json


def resolve_roots(cfg: dict) -> core_merge.Roots:
    return core_merge.resolve_roots(cfg, DEFAULT_ROOTS)


def merge_records(cfg: dict, roots: core_merge.Roots, execute: bool) -> dict:
    return core_merge.merge_records(cfg, roots, execute, pipeline_id=PIPELINE_ID)


def main() -> None:
    main_merge(DOMAIN)


if __name__ == "__main__":
    main()
