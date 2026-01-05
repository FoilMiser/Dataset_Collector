#!/usr/bin/env python3
"""PMC worker wrapper for the safety_incident pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from collector_core.pmc_worker import (  # noqa: E402
    chunk_defaults_from_targets_yaml_v2,
    extract_article_text_v2,
    extract_nxml_v2,
    raw_root_from_targets_yaml,
    run_pmc_worker,
)


def configure_parser(parser) -> None:
    parser.add_argument("--raw-root", default="/data/safety/raw")
    parser.add_argument(
        "--target-id",
        default="pmc_oa_fulltext",
        help="Target id to use for raw output folder",
    )


def output_paths_builder(parsed, targets_path: Path) -> tuple[Path, Path | None]:
    raw_root = raw_root_from_targets_yaml(targets_path, Path(parsed.raw_root).expanduser().resolve())
    out_root = raw_root / "green" / "permissive" / parsed.target_id
    cache_dir = raw_root / "yellow" / "quarantine" / parsed.target_id / "_cache" if parsed.enable_cache else None
    return out_root, cache_dir


if __name__ == "__main__":
    run_pmc_worker(
        pipeline_id="safety_incident",
        pools_root_default="/data/safety/pools",
        log_dir_default="/data/safety/_logs",
        version="2.0",
        configure_parser=configure_parser,
        output_paths_builder=output_paths_builder,
        chunk_defaults_loader=chunk_defaults_from_targets_yaml_v2,
        extract_article_text_fn=extract_article_text_v2,
        extract_nxml_fn=extract_nxml_v2,
        include_pools_root_arg=False,
    )
