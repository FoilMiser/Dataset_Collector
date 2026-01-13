"""HTTP acquisition strategy handlers.

This module provides HTTP download functionality with resume support,
URL validation, and multi-file download capabilities.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from collector_core.acquire.context import (
    AcquireContext,
    InternalMirrorAllowlist,
    StrategyHandler,
)
from collector_core.acquire_limits import (
    build_target_limit_enforcer,
    cleanup_path,
    resolve_result_bytes,
)
from collector_core.dependencies import _try_import, requires
from collector_core.stability import stable_api
from collector_core.utils.paths import ensure_dir, safe_filename
from collector_core.acquire.strategies.http_base import (
    CHUNK_SIZE,
    DownloadResult,
    HttpDownloadBase,
)

# Lazy import for requests
requests = _try_import("requests")


@stable_api
def normalize_download(download: dict[str, Any]) -> dict[str, Any]:
    """Normalize download configuration by merging nested config.

    Args:
        download: Raw download configuration dictionary

    Returns:
        Normalized download configuration with merged config
    """
    d = dict(download or {})
    cfg = d.get("config")

    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in d.items() if k != "config"})
        d = merged

    if d.get("strategy") == "zenodo":
        if not d.get("record_id") and d.get("record"):
            d["record_id"] = d["record"]
        if not d.get("record_id") and isinstance(d.get("record_ids"), list) and d["record_ids"]:
            d["record_id"] = d["record_ids"][0]

    return d


@stable_api
def validate_download_url(
    url: str,
    allow_non_global_hosts: bool,
    internal_mirror_allowlist: InternalMirrorAllowlist | None = None,
) -> tuple[bool, str | None]:
    """Validate that a URL is safe to download from.

    Checks that:
    - The URL uses HTTP or HTTPS scheme
    - The hostname is present
    - The host resolves to global IPs (unless allow_non_global_hosts=True)
    - The host/IP is not blocked (unless in allowlist)

    Args:
        url: URL to validate
        allow_non_global_hosts: If True, allow private/local IP addresses
        internal_mirror_allowlist: Optional allowlist for internal mirrors

    Returns:
        Tuple of (is_valid, error_reason)
    """
    result = HttpDownloadBase.validate_download_url(
        url, allow_non_global_hosts, internal_mirror_allowlist
    )
    return result.allowed, result.reason


def _validate_redirect_chain(
    response: "requests.Response",
    allow_non_global_hosts: bool,
    internal_mirror_allowlist: InternalMirrorAllowlist,
) -> tuple[bool, str | None, str | None]:
    """Validate all URLs in a redirect chain.

    Args:
        response: Response object with potential redirect history
        allow_non_global_hosts: If True, allow private/local IP addresses
        internal_mirror_allowlist: Allowlist for internal mirrors

    Returns:
        Tuple of (is_valid, error_reason, blocked_url)
    """
    redirect_urls: list[str] = []
    for resp in response.history:
        location = (resp.headers or {}).get("Location")
        if location:
            redirect_urls.append(urljoin(resp.url, location))
    redirect_urls.append(response.url)
    result = HttpDownloadBase.validate_redirect_urls(
        redirect_urls, allow_non_global_hosts, internal_mirror_allowlist
    )
    return result.allowed, result.reason, result.blocked_url


def _http_download_with_resume(
    ctx: AcquireContext,
    url: str,
    out_path: Path,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    """Download a file via HTTP with resume support.

    Supports:
    - Resumable downloads using Range headers
    - Retry with exponential backoff for transient errors
    - SHA-256 verification
    - Size verification
    - Redirect chain validation

    Args:
        ctx: Acquire context with configuration
        url: URL to download from
        out_path: Output file path
        expected_size: Optional expected file size for verification
        expected_sha256: Optional expected SHA-256 hash for verification

    Returns:
        Result dictionary with status and metadata
    """
    missing = requires("requests", requests, install="pip install requests")
    if missing:
        raise RuntimeError(missing)
    ensure_dir(out_path.parent)
    temp_path = out_path.with_name(f"{out_path.name}.part")
    max_attempts = max(1, ctx.retry.max_attempts)

    content_length: int | None = None
    resolved_url: str | None = None
    blocked_url: str | None = None
    blocked_reason: str | None = None

    def _stream_response(
        response: "requests.Response", write_mode: str, existing_offset: int
    ) -> None:
        nonlocal content_length, resolved_url
        resolved_url = response.url
        content_length = HttpDownloadBase.parse_content_length(
            response.headers, response.status_code, existing_offset
        )
        with temp_path.open(write_mode) as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)

    def _is_transient_error(exc: Exception) -> bool:
        if isinstance(exc, requests.exceptions.HTTPError):
            status_code = exc.response.status_code if exc.response is not None else None
            return status_code is not None and status_code >= 500
        return isinstance(
            exc,
            (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ContentDecodingError,
                requests.exceptions.TooManyRedirects,
            ),
        )

    validation = HttpDownloadBase.validate_download_url(
        url, ctx.allow_non_global_download_hosts, ctx.internal_mirror_allowlist
    )
    if not validation.allowed:
        return DownloadResult(
            status="error",
            error="blocked_url",
            reason=validation.reason,
            url=url,
        ).to_dict()

    for attempt in range(max_attempts):
        headers: dict[str, str] = {}
        mode = "wb"
        existing = 0
        if temp_path.exists() and ctx.mode.enable_resume:
            existing = temp_path.stat().st_size
        if existing and ctx.mode.enable_resume:
            headers["Range"] = f"bytes={existing}-"
            mode = "ab"
        content_length = None
        resolved_url = None
        try:
            with requests.get(url, stream=True, headers=headers, timeout=(15, 300)) as r:
                r.raise_for_status()
                allowed, reason, blocked = _validate_redirect_chain(
                    r, ctx.allow_non_global_download_hosts, ctx.internal_mirror_allowlist
                )
                if not allowed:
                    blocked_reason = reason
                    blocked_url = blocked
                    break
                if existing and ctx.mode.enable_resume:
                    content_range = r.headers.get("Content-Range")
                    valid_range = HttpDownloadBase.valid_content_range(
                        content_range, existing
                    )
                    if r.status_code == 206:
                        if content_range and not valid_range:
                            raise RuntimeError("Invalid Content-Range for resumed download.")
                        _stream_response(r, mode, existing)
                    elif valid_range:
                        _stream_response(r, mode, existing)
                    else:
                        if r.status_code == 200:
                            with requests.get(url, stream=True, timeout=(15, 300)) as fresh:
                                fresh.raise_for_status()
                                allowed, reason, blocked = _validate_redirect_chain(
                                    fresh,
                                    ctx.allow_non_global_download_hosts,
                                    ctx.internal_mirror_allowlist,
                                )
                                if not allowed:
                                    blocked_reason = reason
                                    blocked_url = blocked
                                    break
                                _stream_response(fresh, "wb", 0)
                        else:
                            raise RuntimeError(
                                "Expected 206 Partial Content or a valid Content-Range for resumed download."
                            )
                else:
                    _stream_response(r, mode, existing)
        except Exception as exc:
            if not _is_transient_error(exc) or attempt >= max_attempts - 1:
                raise
            sleep_time = min(ctx.retry.backoff_base**attempt, ctx.retry.backoff_max)
            time.sleep(sleep_time)
            continue
        break
    if blocked_url:
        temp_path.unlink(missing_ok=True)
        return DownloadResult(
            status="error",
            error="blocked_url",
            reason=blocked_reason,
            url=url,
            blocked_url=blocked_url,
        ).to_dict()
    actual_size = temp_path.stat().st_size
    if content_length is None:
        content_length = actual_size
    if expected_size is not None and actual_size != expected_size:
        temp_path.unlink(missing_ok=True)
        return DownloadResult(
            status="error",
            error="size_mismatch",
            message=(
                f"Expected size {expected_size} bytes "
                f"but downloaded {actual_size} bytes."
            ),
            resolved_url=resolved_url,
            content_length=content_length,
        ).to_dict()
    sha256 = HttpDownloadBase.sha256_file(temp_path)
    if expected_sha256 and sha256.lower() != expected_sha256.lower():
        temp_path.unlink(missing_ok=True)
        return DownloadResult(
            status="error",
            error="sha256_mismatch",
            message="Expected sha256 did not match downloaded content.",
            expected_sha256=expected_sha256,
            sha256=sha256,
            resolved_url=resolved_url,
            content_length=content_length,
        ).to_dict()
    temp_path.replace(out_path)
    result: dict[str, Any] = DownloadResult(
        status="ok",
        path=str(out_path),
        resolved_url=resolved_url,
        content_length=content_length,
        sha256=sha256,
    ).to_dict()
    if ctx.mode.verify_sha256 and "sha256" not in result:
        result["sha256"] = HttpDownloadBase.sha256_file(out_path)
    return result


@stable_api
def handle_http_multi(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Handle HTTP download of multiple URLs.

    Downloads all URLs specified in the download configuration,
    with support for per-file filenames, SHA-256 verification,
    and size limits.

    Args:
        ctx: Acquire context with configuration
        row: Target row with download configuration
        out_dir: Output directory for downloaded files

    Returns:
        List of result dictionaries for each downloaded file
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    urls: list[str] = []
    if download.get("url"):
        urls.append(download["url"])
    urls.extend([u for u in download.get("urls") or [] if u])
    if not urls:
        return [{"status": "error", "error": "missing url"}]
    results: list[dict[str, Any]] = []
    filenames: list[str] = download.get("filenames") or []
    expected_sha256 = download.get("expected_sha256") or download.get("sha256")
    expected_sha256s: list[str | None] | None = None
    expected_sha256_map: dict[str, str] | None = None
    if isinstance(expected_sha256, list):
        expected_sha256s = expected_sha256
    elif isinstance(expected_sha256, dict):
        expected_sha256_map = expected_sha256
    expected_size = download.get("expected_size")
    expected_sizes: list[int | None] | None = None
    expected_size_map: dict[str, int] | None = None
    if isinstance(expected_size, list):
        expected_sizes = [int(s) if s is not None else None for s in expected_size]
    elif isinstance(expected_size, dict):
        expected_size_map = {str(k): int(v) for k, v in expected_size.items() if v is not None}
    for idx, url in enumerate(urls):
        filename = (
            (filenames[idx] if idx < len(filenames) else None)
            or download.get("filename")
            or safe_filename(urlparse(url).path.split("/")[-1])
            or f"payload_{idx}.bin"
        )
        limit_error = enforcer.start_file(filename)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(filename)
        if limit_error:
            results.append(limit_error)
            break
        out_path = out_dir / filename
        if out_path.exists() and not ctx.mode.overwrite:
            result = {"status": "ok", "path": str(out_path), "cached": True}
            size_bytes = resolve_result_bytes(result, out_path)
            limit_error = enforcer.record_bytes(size_bytes, filename)
            if limit_error:
                results.append(limit_error)
            else:
                results.append(result)
            continue
        if not ctx.mode.execute:
            result = {"status": "noop", "path": str(out_path)}
            results.append(result)
            continue
        expected = expected_sha256
        if expected_sha256s is not None:
            expected = expected_sha256s[idx] if idx < len(expected_sha256s) else None
        elif expected_sha256_map is not None:
            expected = expected_sha256_map.get(filename) or expected_sha256_map.get(url)
        size_hint = None
        if expected_sizes is not None:
            size_hint = expected_sizes[idx] if idx < len(expected_sizes) else None
        elif expected_size_map is not None:
            size_hint = expected_size_map.get(filename) or expected_size_map.get(url)
        else:
            size_hint = int(expected_size) if expected_size is not None else None
        limit_error = enforcer.check_size_hint(size_hint, filename)
        if limit_error:
            results.append(limit_error)
            continue
        result = _http_download_with_resume(ctx, url, out_path, size_hint, expected)
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, filename)
        if limit_error:
            if result.get("status") == "ok" and not result.get("cached"):
                cleanup_path(out_path)
            results.append(limit_error)
        else:
            results.append(result)
    return results


@stable_api
def handle_http_single(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Handle HTTP download of a single URL.

    Downloads a single URL from the download configuration,
    with SHA-256 verification and size limit support.

    Args:
        ctx: Acquire context with configuration
        row: Target row with download configuration
        out_dir: Output directory for downloaded file

    Returns:
        List containing a single result dictionary
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    url = download.get("url") or download.get("urls", [None])[0]
    if not url:
        return [{"status": "error", "error": "missing url"}]
    filename = download.get("filename") or safe_filename(urlparse(url).path.split("/")[-1])
    if not filename:
        filename = "payload.bin"
    limit_error = enforcer.start_file(filename)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(filename)
    if limit_error:
        return [limit_error]
    size_hint = download.get("expected_size")
    limit_error = enforcer.check_size_hint(
        int(size_hint) if size_hint is not None else None, filename
    )
    if limit_error:
        return [limit_error]
    out_path = out_dir / filename
    if out_path.exists() and not ctx.mode.overwrite:
        result = {"status": "ok", "path": str(out_path), "cached": True}
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, filename)
        return [limit_error] if limit_error else [result]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_path)}]
    result = _http_download_with_resume(
        ctx, url, out_path, size_hint, download.get("expected_sha256")
    )
    size_bytes = resolve_result_bytes(result, out_path)
    limit_error = enforcer.record_bytes(size_bytes, filename)
    if limit_error:
        if result.get("status") == "ok" and not result.get("cached"):
            cleanup_path(out_path)
        return [limit_error]
    return [result]


@stable_api
def handle_http(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    """Handle HTTP download, routing to single or multi handler.

    Automatically determines whether to use single or multi handler
    based on the number of URLs in the configuration.

    Args:
        ctx: Acquire context with configuration
        row: Target row with download configuration
        out_dir: Output directory for downloaded files

    Returns:
        List of result dictionaries
    """
    download = normalize_download(row.get("download", {}) or {})
    urls: list[str] = []
    if download.get("url"):
        urls.append(download["url"])
    urls.extend([u for u in download.get("urls") or [] if u])
    if len(urls) > 1:
        return handle_http_multi(ctx, row, out_dir)
    return handle_http_single(ctx, row, out_dir)


def resolve_http_handler(variant: str = "multi") -> StrategyHandler:
    """Resolve the HTTP handler based on variant.

    Args:
        variant: Either "single" for single-URL downloads or "multi" for multi-URL

    Returns:
        The appropriate strategy handler function
    """
    if variant == "single":
        return handle_http_single
    return handle_http_multi


def get_multi_handler() -> StrategyHandler:
    """Get the multi-URL HTTP handler.

    Returns:
        The handle_http_multi function
    """
    return handle_http_multi


def get_single_handler() -> StrategyHandler:
    """Get the single-URL HTTP handler.

    Returns:
        The handle_http_single function
    """
    return handle_http_single
