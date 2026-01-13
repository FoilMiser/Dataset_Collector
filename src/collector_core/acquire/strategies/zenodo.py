"""Zenodo acquisition strategy handlers.

This module provides the Zenodo API download strategy for acquiring files
from Zenodo records via their REST API.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

from collector_core.acquire.context import AcquireContext, StrategyHandler
from collector_core.acquire_limits import (
    build_target_limit_enforcer,
    cleanup_path,
    resolve_result_bytes,
)
from collector_core.acquire_strategies import (
    _http_download_with_resume,
    normalize_download,
)
from collector_core.dependencies import _try_import, requires
from collector_core.network_utils import _with_retries
from collector_core.stability import stable_api
from collector_core.utils.paths import ensure_dir, safe_filename

# P0.4: Validation patterns for Zenodo identifiers
_RECORD_ID_PATTERN = re.compile(r"^\d+$")
_DOI_PATTERN = re.compile(r"^10\.\d{4,}/[^\s<>\"]+$")

# Alias for backwards compatibility
safe_name = safe_filename

requests = _try_import("requests")

logger = logging.getLogger(__name__)


def md5_file(path: Path) -> str:
    """Compute MD5 hash of a file.

    Args:
        path: Path to the file to hash.

    Returns:
        The MD5 hex digest of the file contents.
    """
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@stable_api
def handle_zenodo(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    """Handle Zenodo record downloads via the Zenodo REST API.

    This handler fetches metadata for a Zenodo record and downloads all
    associated files. It supports specifying records by:
    - Direct API URL (`api` or `record_url`)
    - Record ID (`record_id`)
    - DOI (`doi`)
    - URL containing `/api/records/` (`url`)

    Args:
        ctx: The acquisition context containing configuration and limits.
        row: The target row containing download configuration.
        out_dir: The output directory for downloaded files.

    Returns:
        A list of result dictionaries, one per file downloaded or attempted.
        Each result contains status information and file metadata.
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    api_url = download.get("api") or download.get("record_url")
    record_id = download.get("record_id")
    doi = download.get("doi")
    url = download.get("url")

    # P0.4: Validate record_id and doi to prevent SSRF
    if record_id and not _RECORD_ID_PATTERN.match(str(record_id)):
        return [{"status": "error", "error": f"Invalid Zenodo record_id: {record_id}"}]
    if doi and not _DOI_PATTERN.match(str(doi)):
        return [{"status": "error", "error": f"Invalid DOI format: {doi}"}]

    if not api_url:
        if record_id:
            api_url = f"https://zenodo.org/api/records/{record_id}"
        elif doi:
            api_url = f"https://zenodo.org/api/records/?q=doi:{doi}"
        elif url and "/api/records/" in url:
            api_url = url
    if not api_url:
        return [{"status": "error", "error": "missing api/record_url/record_id/doi/url"}]
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]

    def _fetch() -> requests.Response:
        resp = requests.get(api_url, timeout=60)
        resp.raise_for_status()
        return resp

    resp = _with_retries(
        _fetch,
        max_attempts=ctx.retry.max_attempts,
        backoff_base=ctx.retry.backoff_base,
        backoff_max=ctx.retry.backoff_max,
    )
    # P1.2B: Handle JSON decode errors from API response
    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        return [{"status": "error", "error": f"Invalid JSON from Zenodo API: {e}"}]
    hits = data.get("hits", {}).get("hits", [])
    if hits and not data.get("files"):
        data = hits[0]
    results: list[dict[str, Any]] = []
    # P1.4B: Fix unsafe [0] access on potentially empty list
    files = data.get("files", [])
    if not files:
        fallback_hits = data.get("hits", {}).get("hits", [])
        if fallback_hits:
            files = fallback_hits[0].get("files", [])
    for f in files:
        link = f.get("links", {}).get("self") or f.get("link")
        if not link:
            continue
        filename = f.get("key") or f.get("name") or safe_name(link)
        limit_error = enforcer.start_file(filename)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(filename)
        if limit_error:
            results.append(limit_error)
            break
        size_hint = f.get("size")
        limit_error = enforcer.check_size_hint(
            int(size_hint) if size_hint is not None else None, filename
        )
        if limit_error:
            results.append(limit_error)
            continue
        out_path = out_dir / filename
        ensure_dir(out_path.parent)
        r = _http_download_with_resume(ctx, link, out_path)
        if ctx.mode.verify_zenodo_md5 and f.get("checksum", "").startswith("md5:"):
            expected_md5 = f["checksum"].split(":", 1)[1]
            if md5_file(out_path) != expected_md5:
                r = {"status": "error", "error": "md5_mismatch"}
        size_bytes = resolve_result_bytes(r, out_path)
        limit_error = enforcer.record_bytes(size_bytes, filename)
        if limit_error:
            if r.get("status") == "ok" and not r.get("cached"):
                cleanup_path(out_path)
            results.append(limit_error)
        else:
            results.append(r)
    return results or [{"status": "noop", "reason": "no files"}]


def get_handler() -> StrategyHandler:
    """Return the Zenodo strategy handler.

    Returns:
        The handle_zenodo function as a StrategyHandler.
    """
    return handle_zenodo
