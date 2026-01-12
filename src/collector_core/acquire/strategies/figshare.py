"""Figshare acquisition strategy handlers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from collector_core.acquire.context import AcquireContext, StrategyHandler
from collector_core.acquire_limits import (
    build_target_limit_enforcer,
    cleanup_path,
    resolve_result_bytes,
)
from collector_core.acquire_strategies import _http_download_with_resume, normalize_download
from collector_core.dependencies import _try_import, requires
from collector_core.network_utils import _with_retries
from collector_core.rate_limit import get_resolver_rate_limiter
from collector_core.stability import stable_api
from collector_core.utils.io import write_json
from collector_core.utils.paths import ensure_dir, safe_filename

# Alias for backwards compatibility
safe_name = safe_filename

requests = _try_import("requests")


@stable_api
def handle_figshare_article(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Handle Figshare article downloads.

    Downloads all files from a Figshare article, including metadata.
    """
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    article_id = download.get("article_id")
    if not article_id and download.get("article_url"):
        try:
            article_id = int(str(download["article_url"]).rstrip("/").split("/")[-1])
        except Exception:
            article_id = None
    if not article_id:
        return [{"status": "error", "error": "missing article_id"}]
    api_base = (download.get("api_base") or "https://api.figshare.com/v2").rstrip("/")
    endpoint = f"{api_base}/articles/{article_id}"
    if not ctx.mode.execute:
        return [{"status": "noop", "article_id": article_id, "path": str(out_dir)}]

    # Get rate limiter from config
    rate_limiter, rate_config = get_resolver_rate_limiter(ctx.cfg, "figshare")

    def _fetch() -> requests.Response:
        # Acquire rate limit token before API request
        if rate_limiter:
            rate_limiter.acquire()
        resp = requests.get(endpoint, timeout=60)
        resp.raise_for_status()
        return resp

    resp = _with_retries(
        _fetch,
        max_attempts=ctx.retry.max_attempts,
        backoff_base=ctx.retry.backoff_base,
        backoff_max=ctx.retry.backoff_max,
        retry_on_429=rate_config.retry_on_429,
        retry_on_403=rate_config.retry_on_403,
    )
    meta = resp.json()
    files = meta.get("files", []) or []
    results: list[dict[str, Any]] = []
    for idx, fmeta in enumerate(files):
        download_url = fmeta.get("download_url") or (fmeta.get("links") or {}).get("download")
        if not download_url:
            results.append(
                {"status": "error", "error": "missing_download_url", "file": fmeta.get("name")}
            )
            continue
        fname = safe_name(fmeta.get("name") or fmeta.get("id") or f"figshare_file_{idx}")
        limit_error = enforcer.start_file(fname)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(fname)
        if limit_error:
            results.append(limit_error)
            break
        expected_size = fmeta.get("size")
        limit_error = enforcer.check_size_hint(
            int(expected_size) if expected_size is not None else None, fname
        )
        if limit_error:
            results.append(limit_error)
            continue
        out_path = out_dir / fname
        result = _http_download_with_resume(ctx, download_url, out_path, expected_size)
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, fname)
        if limit_error:
            if result.get("status") == "ok" and not result.get("cached"):
                cleanup_path(out_path)
            results.append(limit_error)
        else:
            results.append(result)
    write_json(out_dir / "figshare_article.json", meta)
    return results


# Alias for backwards compatibility
handle_figshare = handle_figshare_article


@stable_api
def handle_figshare_files(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Handle Figshare files-only downloads.

    Downloads files from a Figshare article using the files API endpoint.
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    article_id = download.get("article_id") or download.get("id")
    api = download.get("api") or (
        f"https://api.figshare.com/v2/articles/{article_id}/files" if article_id else None
    )
    if not article_id or not api:
        return [{"status": "error", "error": "missing article_id"}]
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        return [{"status": "error", "error": missing}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]

    # Get rate limiter from config
    rate_limiter, rate_config = get_resolver_rate_limiter(ctx.cfg, "figshare")

    def _fetch() -> requests.Response:
        # Acquire rate limit token before API request
        if rate_limiter:
            rate_limiter.acquire()
        resp = requests.get(api, timeout=120)
        resp.raise_for_status()
        return resp

    resp = _with_retries(
        _fetch,
        max_attempts=ctx.retry.max_attempts,
        backoff_base=ctx.retry.backoff_base,
        backoff_max=ctx.retry.backoff_max,
        retry_on_429=rate_config.retry_on_429,
        retry_on_403=rate_config.retry_on_403,
    )
    files = resp.json() or []
    ensure_dir(out_dir)
    results: list[dict[str, Any]] = []
    for f in files:
        link = f.get("download_url") or (f.get("links") or {}).get("download")
        if not link:
            continue
        filename = safe_name(f.get("name") or f.get("id") or str(article_id))
        out_path = out_dir / filename
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
        result = _http_download_with_resume(ctx, link, out_path)
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, filename)
        if limit_error:
            if result.get("status") == "ok" and not result.get("cached"):
                cleanup_path(out_path)
            results.append(limit_error)
        else:
            results.append(result)
    return results or [{"status": "noop", "reason": "no files"}]


def get_handler() -> StrategyHandler:
    """Return the default Figshare strategy handler."""
    return handle_figshare_article


def resolve_figshare_handler(variant: str) -> StrategyHandler:
    """Return Figshare handler for the specified variant.

    Args:
        variant: Either "article" (default) or "files".

    Returns:
        The appropriate strategy handler function.
    """
    if variant == "files":
        return handle_figshare_files
    return handle_figshare_article
