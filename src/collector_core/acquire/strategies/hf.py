"""Hugging Face datasets acquisition strategy handlers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from collector_core.acquire.context import AcquireContext, StrategyHandler
from collector_core.acquire_limits import (
    build_target_limit_enforcer,
    cleanup_path,
    resolve_result_bytes,
)
from collector_core.acquire_strategies import normalize_download
from collector_core.stability import stable_api
from collector_core.utils.paths import ensure_dir, safe_filename


@stable_api
def handle_hf_datasets(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Download datasets from Hugging Face Hub.

    Supports downloading specific splits or the entire dataset.
    Uses the `datasets` library to load and save datasets to disk.

    Args:
        ctx: The acquire context with configuration and limits.
        row: The target row containing download configuration.
        out_dir: The output directory for downloaded files.

    Returns:
        A list of result dictionaries with status and path information.
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    dataset_id = download.get("dataset_id")
    if not dataset_id:
        return [{"status": "error", "error": "missing dataset_id"}]
    splits = download.get("splits") or download.get("split")
    if isinstance(splits, str):
        splits = [splits]
    load_kwargs = download.get("load_kwargs", {}) or {}
    cfg = download.get("config")
    hf_name = cfg if isinstance(cfg, str) else None
    if hf_name and "name" not in load_kwargs:
        load_kwargs["name"] = hf_name
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    try:
        from datasets import load_dataset  # type: ignore
    except Exception as e:  # pragma: no cover - optional dep
        return [{"status": "error", "error": f"datasets import failed: {e}"}]

    results: list[dict[str, Any]] = []
    ensure_dir(out_dir)
    if splits:
        for sp in splits:
            file_label = f"{dataset_id}:{sp}"
            limit_error = enforcer.start_file(file_label)
            if limit_error:
                results.append(limit_error)
                break
            limit_error = enforcer.check_remaining_bytes(file_label)
            if limit_error:
                results.append(limit_error)
                break
            ds = load_dataset(dataset_id, split=sp, **load_kwargs)
            sp_dir = out_dir / f"split_{safe_filename(sp)}"
            ds.save_to_disk(str(sp_dir))
            result = {"status": "ok", "dataset_id": dataset_id, "split": sp, "path": str(sp_dir)}
            size_bytes = resolve_result_bytes(result, sp_dir)
            limit_error = enforcer.record_bytes(size_bytes, file_label)
            if limit_error:
                cleanup_path(sp_dir)
                results.append(limit_error)
            else:
                results.append(result)
    else:
        file_label = dataset_id
        limit_error = enforcer.start_file(file_label)
        if limit_error:
            return [limit_error]
        limit_error = enforcer.check_remaining_bytes(file_label)
        if limit_error:
            return [limit_error]
        ds = load_dataset(dataset_id, **load_kwargs)
        ds_path = out_dir / "hf_dataset"
        ds.save_to_disk(str(ds_path))
        result = {"status": "ok", "dataset_id": dataset_id, "path": str(ds_path)}
        size_bytes = resolve_result_bytes(result, ds_path)
        limit_error = enforcer.record_bytes(size_bytes, file_label)
        if limit_error:
            cleanup_path(ds_path)
            return [limit_error]
        results.append(result)
    return results


def get_handler() -> StrategyHandler:
    """Return the Hugging Face datasets strategy handler."""
    return handle_hf_datasets
