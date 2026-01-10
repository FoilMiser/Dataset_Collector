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
    DEFAULT_STRATEGY_HANDLERS,
    RootsDefaults,
    handle_figshare_article,
    make_github_release_handler,
    run_acquire_worker,
)

__all__ = ["main", "VERSION"]

GITHUB_RELEASE_HANDLER = make_github_release_handler("chem-corpus-acquire")

STRATEGY_HANDLERS = {
    **DEFAULT_STRATEGY_HANDLERS,
    "figshare": handle_figshare_article,
    "github_release": GITHUB_RELEASE_HANDLER,
}

DEFAULTS = RootsDefaults(
    raw_root="/data/chem/raw",
    manifests_root="/data/chem/_manifests",
    ledger_root="/data/chem/_ledger",
    logs_root="/data/chem/_logs",
)


def main() -> None:
    run_acquire_worker(
        defaults=DEFAULTS,
        targets_yaml_label="targets_chem.yaml",
        strategy_handlers=STRATEGY_HANDLERS,
    )


if __name__ == "__main__":
    main()
