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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pathlib import Path
from typing import Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.acquire_strategies import (
    AcquireContext,
    RootsDefaults,
    handle_dataverse,
    handle_figshare_article,
    handle_ftp,
    handle_git,
    handle_hf_datasets,
    handle_http_multi,
    handle_zenodo,
    make_github_release_handler,
    resolve_license_pool,
    run_acquire_worker,
)

__all__ = ["main", "VERSION"]

GITHUB_RELEASE_HANDLER = make_github_release_handler("code-corpus-acquire")

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
    raw_root="/data/code/raw",
    manifests_root="/data/code/_manifests",
    logs_root="/data/code/_logs",
)


def should_run_code_worker(row: dict[str, Any]) -> bool:
    data_types = row.get("data_type") or row.get("data_types") or []
    if isinstance(data_types, str):
        data_types = [data_types]
    flags = row.get("code_worker", {}) or {}
    return "code" in [str(t).lower() for t in data_types] or bool(flags.get("enabled", False))


def run_code_worker(ctx: AcquireContext, row: dict[str, Any], out_dir: Path, bucket: str) -> dict[str, Any]:
    try:
        from code_worker import run_extraction  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        return {"status": "error", "error": f"code_worker_import_failed: {exc}"}

    cfg = ctx.cfg or {}
    globals_cfg = cfg.get("globals", {}) if isinstance(cfg, dict) else {}
    processing_defaults = globals_cfg.get("code_processing_defaults", {}) or {}
    target_cfg: dict[str, Any] = {}
    for t in cfg.get("targets", []) if isinstance(cfg, dict) else []:
        if t.get("id") == row.get("id"):
            target_cfg = t
            break
    processing_overrides = target_cfg.get("code_processing", {}) if isinstance(target_cfg, dict) else {}
    if processing_overrides:
        processing_defaults = {**processing_defaults, **processing_overrides}
    sharding_cfg = globals_cfg.get("sharding", {}) or {}
    routing = row.get("routing") or row.get("code_routing") or target_cfg.get("routing") or {}
    download_cfg = row.get("download", {}) or {}
    source_url = download_cfg.get("repo") or download_cfg.get("repo_url") or download_cfg.get("url")
    license_spdx = row.get("resolved_spdx") or (row.get("license_evidence", {}) or {}).get("spdx_hint")
    pitches_root = globals_cfg.get("pitches_root")

    summary = run_extraction(
        input_dir=out_dir,
        output_dir=out_dir / "shards",
        target_id=row["id"],
        license_profile=resolve_license_pool(row),
        license_spdx=license_spdx,
        bucket=bucket,
        routing=routing,
        processing_defaults=processing_defaults,
        sharding=sharding_cfg,
        source_url=source_url,
        pitches_root=Path(pitches_root) if pitches_root else None,
    )
    return {"status": "ok", "path": str(out_dir), "code_worker": summary}


def code_postprocess(
    ctx: AcquireContext,
    row: dict[str, Any],
    out_dir: Path,
    bucket: str,
    manifest: dict[str, Any],
) -> dict[str, Any] | None:
    if not ctx.mode.execute:
        return None
    if not should_run_code_worker(row):
        return None
    if not any(result.get("status") == "ok" for result in manifest.get("results", [])):
        return None
    return {"code_worker": run_code_worker(ctx, row, out_dir, bucket)}


def main() -> None:
    run_acquire_worker(
        defaults=DEFAULTS,
        targets_yaml_label="targets_code.yaml",
        strategy_handlers=STRATEGY_HANDLERS,
        postprocess=code_postprocess,
    )


if __name__ == "__main__":
    main()
