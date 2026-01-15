"""Dataverse acquisition strategy handlers."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from collector_core.acquire.context import AcquireContext, StrategyHandler
from collector_core.acquire_limits import build_target_limit_enforcer
from collector_core.acquire_strategies import normalize_download
from collector_core.dependencies import _try_import, requires
from collector_core.stability import stable_api
from collector_core.utils.hash import sha256_file
from collector_core.utils.paths import ensure_dir, safe_filename

requests = _try_import("requests")


@stable_api
def handle_dataverse(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Handle Dataverse download strategy.

    Downloads files from a Dataverse repository using the API.

    Args:
        ctx: The acquire context with configuration and limits.
        row: The target row containing download configuration with:
            - persistent_id or pid: The persistent identifier for the dataset
            - instance: Dataverse instance URL (default: https://dataverse.harvard.edu)
            - expected_sha256: Optional SHA256 hash for verification
        out_dir: Output directory for downloaded files.

    Returns:
        List of result dictionaries, one per downloaded file.
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    pid = download.get("persistent_id") or download.get("pid")
    instance = download.get("instance") or "https://dataverse.harvard.edu"
    if not pid:
        return [{"status": "error", "error": "missing persistent_id"}]
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    limit_error = enforcer.start_file(pid)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(pid)
    if limit_error:
        return [limit_error]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    url = f"{instance}/api/access/dvobject/{pid}"
    resp = requests.get(url, allow_redirects=True, timeout=60)
    resp.raise_for_status()
    size_hint = resp.headers.get("Content-Length")
    limit_error = enforcer.check_size_hint(int(size_hint) if size_hint else None, pid)
    if limit_error:
        return [limit_error]
    filename = safe_filename(urlparse(resp.url).path.split("/")[-1] or pid)
    out_path = out_dir / filename
    ensure_dir(out_path.parent)
    temp_path = out_path.with_name(f"{out_path.name}.part")
    with temp_path.open("wb") as f:
        f.write(resp.content)
    content_length = temp_path.stat().st_size
    sha256 = sha256_file(temp_path)
    if sha256 is None:
        sha256 = ""
    expected_sha256 = download.get("expected_sha256")
    if expected_sha256 and sha256.lower() != expected_sha256.lower():
        temp_path.unlink(missing_ok=True)
        return [
            {
                "status": "error",
                "error": "sha256_mismatch",
                "message": "Expected sha256 did not match downloaded content.",
                "expected_sha256": expected_sha256,
                "sha256": sha256,
                "resolved_url": resp.url,
                "content_length": content_length,
            }
        ]
    limit_error = enforcer.record_bytes(content_length, filename)
    if limit_error:
        temp_path.unlink(missing_ok=True)
        return [limit_error]
    temp_path.replace(out_path)
    return [
        {
            "status": "ok",
            "path": str(out_path),
            "resolved_url": resp.url,
            "content_length": content_length,
            "sha256": sha256,
        }
    ]


def get_handler() -> StrategyHandler:
    """Return the Dataverse strategy handler function."""
    return handle_dataverse
