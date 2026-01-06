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

STRATEGY_HANDLERS = {
    "http": handle_http_multi,
    "ftp": handle_ftp,
    "git": handle_git,
    "zenodo": handle_zenodo,
    "dataverse": handle_dataverse,
    "huggingface_datasets": handle_hf_datasets,
}

DEFAULTS = RootsDefaults(
    raw_root="/data/cyber/raw",
    manifests_root="/data/cyber/_manifests",
    logs_root="/data/cyber/_logs",
)


def main() -> None:
    run_acquire_worker(
        defaults=DEFAULTS,
        targets_yaml_label="targets_cyber.yaml",
        strategy_handlers=STRATEGY_HANDLERS,
    )


if __name__ == "__main__":
    main()
