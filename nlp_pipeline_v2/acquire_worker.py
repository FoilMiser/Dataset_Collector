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

from collector_core.__version__ import __version__ as VERSION

from collector_core.acquire_strategies import (
    RootsDefaults,
    handle_dataverse,
    handle_figshare_article,
    handle_ftp,
    handle_git,
    handle_hf_datasets,
    handle_http_multi,
    handle_zenodo,
    make_github_release_handler,
    run_acquire_worker,
)

__all__ = ["main", "VERSION"]

GITHUB_RELEASE_HANDLER = make_github_release_handler("nlp-corpus-acquire")

STRATEGY_HANDLERS = {
    "http": handle_http_multi,
    "ftp": handle_ftp,
    "git": handle_git,
    "zenodo": handle_zenodo,
    "dataverse": handle_dataverse,
    "figshare": handle_figshare_article,
    "github_release": GITHUB_RELEASE_HANDLER,
    "huggingface_datasets": handle_hf_datasets,
}

DEFAULTS = RootsDefaults(
    raw_root="/data/nlp/raw",
    manifests_root="/data/nlp/_manifests",
    logs_root="/data/nlp/_logs",
)


def main() -> None:
    run_acquire_worker(
        defaults=DEFAULTS,
        targets_yaml_label="targets_nlp.yaml",
        strategy_handlers=STRATEGY_HANDLERS,
    )


if __name__ == "__main__":
    main()
