#!/usr/bin/env python3
"""
acquire_worker.py (v2.0)

Replaces download_worker.py with the v2 raw layout:
  raw/{green|yellow}/{license_pool}/{target_id}/...

Reads queue rows emitted by pipeline_driver.py and downloads payloads using the
configured strategy. Dry-run by default; pass --execute to write files. After a
successful run it writes a per-target `acquire_done.json` under the manifests
root.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector_core.__version__ import __version__ as VERSION
from collector_core.acquire_strategies import (
    RootsDefaults,
    handle_dataverse,
    handle_ftp,
    handle_git,
    handle_hf_datasets,
    handle_http_multi,
    handle_zenodo,
    run_acquire_worker,
)

__all__ = ["main", "VERSION"]

STRATEGY_HANDLERS = {
    "http": handle_http_multi,
    "ftp": handle_ftp,
    "git": handle_git,
    "zenodo": handle_zenodo,
    "dataverse": handle_dataverse,
    "huggingface_datasets": handle_hf_datasets,
}

DEFAULTS = RootsDefaults(
    raw_root="/data/bio/raw",
    manifests_root="/data/bio/_manifests",
    logs_root="/data/bio/_logs",
)


def main() -> None:
    run_acquire_worker(
        defaults=DEFAULTS,
        targets_yaml_label="targets_biology.yaml",
        strategy_handlers=STRATEGY_HANDLERS,
    )


if __name__ == "__main__":
    main()
