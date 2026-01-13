"""Async HTTP acquisition strategy handlers.

This module provides async HTTP download functionality with resume support,
URL validation, bounded concurrency, and retry with exponential backoff.

The async functionality is optional and requires either aiohttp or httpx
to be installed. Use the optional dependency group 'async' to install:

    pip install dataset-collector[async]

Usage:
    from collector_core.acquire.strategies.http_async import (
        async_download_with_resume,
        get_async_handler,
        is_async_available,
    )

    # Check if async is available
    if is_async_available():
        handler = get_async_handler()
        results = await handler(ctx, row, out_dir)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any, TYPE_CHECKING
from urllib.parse import urljoin, urlparse

from collector_core.acquire.context import (
    AcquireContext,
    InternalMirrorAllowlist,
)
from collector_core.acquire.strategies.http import normalize_download
from collector_core.acquire.strategies.http_base import (
    CHUNK_SIZE,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_READ_TIMEOUT,
    DownloadResult,
    HttpDownloadBase,
)
from collector_core.acquire_limits import (
    build_target_limit_enforcer,
    cleanup_path,
    resolve_result_bytes,
)
from collector_core.dependencies import _try_import, requires
from collector_core.stability import stable_api
from collector_core.utils.paths import ensure_dir, safe_filename

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Lazy imports for async HTTP libraries (optional dependencies)
aiohttp = _try_import("aiohttp")
httpx = _try_import("httpx")

# Type alias for async strategy handler
AsyncStrategyHandler = Callable[
    [AcquireContext, dict[str, Any], Path],
    Coroutine[Any, Any, list[dict[str, Any]]],
]

# Default concurrency settings
DEFAULT_MAX_CONCURRENT_DOWNLOADS = 5


def is_async_available() -> bool:
    """Check if async HTTP functionality is available.

    Returns True if either aiohttp or httpx is installed.

    Returns:
        True if async downloads are supported
    """
    return aiohttp is not None or httpx is not None


def _get_async_library() -> str | None:
    """Get the name of the available async HTTP library.

    Prefers aiohttp over httpx if both are installed.

    Returns:
        Library name ('aiohttp' or 'httpx') or None if neither available
    """
    if aiohttp is not None:
        return "aiohttp"
    if httpx is not None:
        return "httpx"
    return None


def _check_async_dependency() -> str | None:
    """Check if async dependencies are available.

    Returns:
        Error message string if dependencies missing, None otherwise
    """
    if is_async_available():
        return None
    return (
        "Async HTTP downloads require either aiohttp or httpx. "
        "Install with: pip install aiohttp  OR  pip install httpx"
    )


class _AsyncDownloadState:
    """Mutable state container for async download operations."""

    def __init__(self) -> None:
        self.content_length: int | None = None
        self.resolved_url: str | None = None
        self.blocked_url: str | None = None
        self.blocked_reason: str | None = None


def _is_transient_status_code(status_code: int) -> bool:
    """Check if HTTP status code indicates a transient/retryable error.

    Args:
        status_code: HTTP response status code

    Returns:
        True if the error is likely transient
    """
    # 5xx server errors are transient
    if status_code >= 500:
        return True
    # 429 Too Many Requests is transient
    if status_code == 429:
        return True
    # 408 Request Timeout is transient
    if status_code == 408:
        return True
    return False


async def _validate_redirect_chain_aiohttp(
    response: "aiohttp.ClientResponse",
    allow_non_global_hosts: bool,
    internal_mirror_allowlist: InternalMirrorAllowlist,
) -> tuple[bool, str | None, str | None]:
    """Validate all URLs in a redirect chain (aiohttp version).

    Args:
        response: aiohttp ClientResponse object
        allow_non_global_hosts: If True, allow private/local IP addresses
        internal_mirror_allowlist: Allowlist for internal mirrors

    Returns:
        Tuple of (is_valid, error_reason, blocked_url)
    """
    redirect_urls: list[str] = []

    # aiohttp stores redirect history in response.history
    for hist_resp in response.history:
        location = hist_resp.headers.get("Location")
        if location:
            redirect_urls.append(urljoin(str(hist_resp.url), location))

    redirect_urls.append(str(response.url))

    result = HttpDownloadBase.validate_redirect_urls(
        redirect_urls, allow_non_global_hosts, internal_mirror_allowlist
    )
    return result.allowed, result.reason, result.blocked_url


async def _validate_redirect_chain_httpx(
    response: "httpx.Response",
    allow_non_global_hosts: bool,
    internal_mirror_allowlist: InternalMirrorAllowlist,
) -> tuple[bool, str | None, str | None]:
    """Validate all URLs in a redirect chain (httpx version).

    Args:
        response: httpx Response object
        allow_non_global_hosts: If True, allow private/local IP addresses
        internal_mirror_allowlist: Allowlist for internal mirrors

    Returns:
        Tuple of (is_valid, error_reason, blocked_url)
    """
    redirect_urls: list[str] = []

    # httpx stores redirect history in response.history
    for hist_resp in response.history:
        location = hist_resp.headers.get("Location")
        if location:
            redirect_urls.append(urljoin(str(hist_resp.url), location))

    redirect_urls.append(str(response.url))

    result = HttpDownloadBase.validate_redirect_urls(
        redirect_urls, allow_non_global_hosts, internal_mirror_allowlist
    )
    return result.allowed, result.reason, result.blocked_url


def _is_transient_exception_aiohttp(exc: Exception) -> bool:
    """Check if an aiohttp exception is transient/retryable.

    Args:
        exc: The exception to check

    Returns:
        True if the exception is likely transient
    """
    if aiohttp is None:
        return False

    # aiohttp-specific transient exceptions
    transient_types = (
        aiohttp.ClientConnectionError,
        aiohttp.ServerTimeoutError,
        aiohttp.ServerDisconnectedError,
        asyncio.TimeoutError,
    )

    if isinstance(exc, aiohttp.ClientResponseError):
        return _is_transient_status_code(exc.status)

    return isinstance(exc, transient_types)


def _is_transient_exception_httpx(exc: Exception) -> bool:
    """Check if an httpx exception is transient/retryable.

    Args:
        exc: The exception to check

    Returns:
        True if the exception is likely transient
    """
    if httpx is None:
        return False

    # httpx-specific transient exceptions
    transient_types = (
        httpx.ConnectError,
        httpx.ReadTimeout,
        httpx.WriteTimeout,
        httpx.ConnectTimeout,
        httpx.PoolTimeout,
        asyncio.TimeoutError,
    )

    if isinstance(exc, httpx.HTTPStatusError):
        return _is_transient_status_code(exc.response.status_code)

    return isinstance(exc, transient_types)


async def _stream_response_aiohttp(
    response: "aiohttp.ClientResponse",
    temp_path: Path,
    write_mode: str,
    existing_offset: int,
    state: _AsyncDownloadState,
) -> None:
    """Stream response content to file (aiohttp version).

    Args:
        response: aiohttp ClientResponse object
        temp_path: Path to write content to
        write_mode: File write mode ('wb' or 'ab')
        existing_offset: Bytes already downloaded
        state: Mutable state container to update
    """
    state.resolved_url = str(response.url)
    headers = {k: v for k, v in response.headers.items()}
    state.content_length = HttpDownloadBase.parse_content_length(
        headers, response.status, existing_offset
    )

    with temp_path.open(write_mode) as f:
        async for chunk in response.content.iter_chunked(CHUNK_SIZE):
            if chunk:
                f.write(chunk)


async def _stream_response_httpx(
    response: "httpx.Response",
    temp_path: Path,
    write_mode: str,
    existing_offset: int,
    state: _AsyncDownloadState,
) -> None:
    """Stream response content to file (httpx version).

    Args:
        response: httpx Response object
        temp_path: Path to write content to
        write_mode: File write mode ('wb' or 'ab')
        existing_offset: Bytes already downloaded
        state: Mutable state container to update
    """
    state.resolved_url = str(response.url)
    headers = {k: v for k, v in response.headers.items()}
    state.content_length = HttpDownloadBase.parse_content_length(
        headers, response.status_code, existing_offset
    )

    with temp_path.open(write_mode) as f:
        async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
            if chunk:
                f.write(chunk)


async def _async_download_aiohttp(
    ctx: AcquireContext,
    url: str,
    out_path: Path,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> dict[str, Any]:
    """Async download with resume support using aiohttp.

    Args:
        ctx: Acquire context with configuration
        url: URL to download from
        out_path: Output file path
        expected_size: Optional expected file size for verification
        expected_sha256: Optional expected SHA-256 hash for verification
        semaphore: Optional semaphore for concurrency control

    Returns:
        Result dictionary with status and metadata
    """
    missing = requires("aiohttp", aiohttp, install="pip install aiohttp")
    if missing:
        raise RuntimeError(missing)

    ensure_dir(out_path.parent)
    temp_path = out_path.with_name(f"{out_path.name}.part")
    max_attempts = max(1, ctx.retry.max_attempts)

    state = _AsyncDownloadState()

    # Validate initial URL
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

    timeout = aiohttp.ClientTimeout(
        connect=DEFAULT_CONNECT_TIMEOUT,
        total=None,  # No total timeout for large files
        sock_read=DEFAULT_READ_TIMEOUT,
    )

    async def _do_download() -> None:
        nonlocal state

        for attempt in range(max_attempts):
            headers: dict[str, str] = {}
            mode = "wb"
            existing = 0

            if temp_path.exists() and ctx.mode.enable_resume:
                existing = temp_path.stat().st_size
            if existing and ctx.mode.enable_resume:
                headers["Range"] = f"bytes={existing}-"
                mode = "ab"

            state = _AsyncDownloadState()

            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers=headers) as response:
                        response.raise_for_status()

                        # Validate redirect chain
                        allowed, reason, blocked = await _validate_redirect_chain_aiohttp(
                            response,
                            ctx.allow_non_global_download_hosts,
                            ctx.internal_mirror_allowlist,
                        )
                        if not allowed:
                            state.blocked_reason = reason
                            state.blocked_url = blocked
                            return

                        resp_headers = {k: v for k, v in response.headers.items()}
                        content_range = resp_headers.get("Content-Range")
                        valid_range = HttpDownloadBase.valid_content_range(
                            content_range, existing
                        )

                        if existing and ctx.mode.enable_resume:
                            if response.status == 206:
                                if content_range and not valid_range:
                                    raise RuntimeError(
                                        "Invalid Content-Range for resumed download."
                                    )
                                await _stream_response_aiohttp(
                                    response, temp_path, mode, existing, state
                                )
                            elif valid_range:
                                await _stream_response_aiohttp(
                                    response, temp_path, mode, existing, state
                                )
                            else:
                                # Server doesn't support resume, start fresh
                                if response.status == 200:
                                    async with session.get(url) as fresh:
                                        fresh.raise_for_status()
                                        allowed, reason, blocked = (
                                            await _validate_redirect_chain_aiohttp(
                                                fresh,
                                                ctx.allow_non_global_download_hosts,
                                                ctx.internal_mirror_allowlist,
                                            )
                                        )
                                        if not allowed:
                                            state.blocked_reason = reason
                                            state.blocked_url = blocked
                                            return
                                        await _stream_response_aiohttp(
                                            fresh, temp_path, "wb", 0, state
                                        )
                                else:
                                    raise RuntimeError(
                                        "Expected 206 Partial Content or valid "
                                        "Content-Range for resumed download."
                                    )
                        else:
                            await _stream_response_aiohttp(
                                response, temp_path, mode, existing, state
                            )
                # Success - break out of retry loop
                return

            except Exception as exc:
                if not _is_transient_exception_aiohttp(exc) or attempt >= max_attempts - 1:
                    raise
                sleep_time = min(
                    ctx.retry.backoff_base ** attempt,
                    ctx.retry.backoff_max,
                )
                logger.debug(
                    f"Transient error on attempt {attempt + 1}/{max_attempts}, "
                    f"retrying in {sleep_time:.1f}s: {exc}"
                )
                await asyncio.sleep(sleep_time)

    # Execute with optional semaphore for concurrency control
    if semaphore:
        async with semaphore:
            await _do_download()
    else:
        await _do_download()

    # Check for blocked redirect
    if state.blocked_url:
        temp_path.unlink(missing_ok=True)
        return DownloadResult(
            status="error",
            error="blocked_url",
            reason=state.blocked_reason,
            url=url,
            blocked_url=state.blocked_url,
        ).to_dict()

    # Verify download
    actual_size = temp_path.stat().st_size
    if state.content_length is None:
        state.content_length = actual_size

    if expected_size is not None and actual_size != expected_size:
        temp_path.unlink(missing_ok=True)
        return DownloadResult(
            status="error",
            error="size_mismatch",
            message=(
                f"Expected size {expected_size} bytes "
                f"but downloaded {actual_size} bytes."
            ),
            resolved_url=state.resolved_url,
            content_length=state.content_length,
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
            resolved_url=state.resolved_url,
            content_length=state.content_length,
        ).to_dict()

    # Move temp file to final location
    temp_path.replace(out_path)

    result: dict[str, Any] = DownloadResult(
        status="ok",
        path=str(out_path),
        resolved_url=state.resolved_url,
        content_length=state.content_length,
        sha256=sha256,
    ).to_dict()

    if ctx.mode.verify_sha256 and "sha256" not in result:
        result["sha256"] = HttpDownloadBase.sha256_file(out_path)

    return result


async def _async_download_httpx(
    ctx: AcquireContext,
    url: str,
    out_path: Path,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> dict[str, Any]:
    """Async download with resume support using httpx.

    Args:
        ctx: Acquire context with configuration
        url: URL to download from
        out_path: Output file path
        expected_size: Optional expected file size for verification
        expected_sha256: Optional expected SHA-256 hash for verification
        semaphore: Optional semaphore for concurrency control

    Returns:
        Result dictionary with status and metadata
    """
    missing = requires("httpx", httpx, install="pip install httpx")
    if missing:
        raise RuntimeError(missing)

    ensure_dir(out_path.parent)
    temp_path = out_path.with_name(f"{out_path.name}.part")
    max_attempts = max(1, ctx.retry.max_attempts)

    state = _AsyncDownloadState()

    # Validate initial URL
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

    timeout = httpx.Timeout(
        connect=DEFAULT_CONNECT_TIMEOUT,
        read=DEFAULT_READ_TIMEOUT,
        write=None,
        pool=None,
    )

    async def _do_download() -> None:
        nonlocal state

        for attempt in range(max_attempts):
            headers: dict[str, str] = {}
            mode = "wb"
            existing = 0

            if temp_path.exists() and ctx.mode.enable_resume:
                existing = temp_path.stat().st_size
            if existing and ctx.mode.enable_resume:
                headers["Range"] = f"bytes={existing}-"
                mode = "ab"

            state = _AsyncDownloadState()

            try:
                async with httpx.AsyncClient(
                    timeout=timeout, follow_redirects=True
                ) as client:
                    async with client.stream("GET", url, headers=headers) as response:
                        response.raise_for_status()

                        # Validate redirect chain
                        allowed, reason, blocked = await _validate_redirect_chain_httpx(
                            response,
                            ctx.allow_non_global_download_hosts,
                            ctx.internal_mirror_allowlist,
                        )
                        if not allowed:
                            state.blocked_reason = reason
                            state.blocked_url = blocked
                            return

                        resp_headers = {k: v for k, v in response.headers.items()}
                        content_range = resp_headers.get("Content-Range")
                        valid_range = HttpDownloadBase.valid_content_range(
                            content_range, existing
                        )

                        if existing and ctx.mode.enable_resume:
                            if response.status_code == 206:
                                if content_range and not valid_range:
                                    raise RuntimeError(
                                        "Invalid Content-Range for resumed download."
                                    )
                                await _stream_response_httpx(
                                    response, temp_path, mode, existing, state
                                )
                            elif valid_range:
                                await _stream_response_httpx(
                                    response, temp_path, mode, existing, state
                                )
                            else:
                                # Server doesn't support resume, start fresh
                                if response.status_code == 200:
                                    async with client.stream("GET", url) as fresh:
                                        fresh.raise_for_status()
                                        allowed, reason, blocked = (
                                            await _validate_redirect_chain_httpx(
                                                fresh,
                                                ctx.allow_non_global_download_hosts,
                                                ctx.internal_mirror_allowlist,
                                            )
                                        )
                                        if not allowed:
                                            state.blocked_reason = reason
                                            state.blocked_url = blocked
                                            return
                                        await _stream_response_httpx(
                                            fresh, temp_path, "wb", 0, state
                                        )
                                else:
                                    raise RuntimeError(
                                        "Expected 206 Partial Content or valid "
                                        "Content-Range for resumed download."
                                    )
                        else:
                            await _stream_response_httpx(
                                response, temp_path, mode, existing, state
                            )
                # Success - break out of retry loop
                return

            except Exception as exc:
                if not _is_transient_exception_httpx(exc) or attempt >= max_attempts - 1:
                    raise
                sleep_time = min(
                    ctx.retry.backoff_base ** attempt,
                    ctx.retry.backoff_max,
                )
                logger.debug(
                    f"Transient error on attempt {attempt + 1}/{max_attempts}, "
                    f"retrying in {sleep_time:.1f}s: {exc}"
                )
                await asyncio.sleep(sleep_time)

    # Execute with optional semaphore for concurrency control
    if semaphore:
        async with semaphore:
            await _do_download()
    else:
        await _do_download()

    # Check for blocked redirect
    if state.blocked_url:
        temp_path.unlink(missing_ok=True)
        return DownloadResult(
            status="error",
            error="blocked_url",
            reason=state.blocked_reason,
            url=url,
            blocked_url=state.blocked_url,
        ).to_dict()

    # Verify download
    actual_size = temp_path.stat().st_size
    if state.content_length is None:
        state.content_length = actual_size

    if expected_size is not None and actual_size != expected_size:
        temp_path.unlink(missing_ok=True)
        return DownloadResult(
            status="error",
            error="size_mismatch",
            message=(
                f"Expected size {expected_size} bytes "
                f"but downloaded {actual_size} bytes."
            ),
            resolved_url=state.resolved_url,
            content_length=state.content_length,
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
            resolved_url=state.resolved_url,
            content_length=state.content_length,
        ).to_dict()

    # Move temp file to final location
    temp_path.replace(out_path)

    result: dict[str, Any] = DownloadResult(
        status="ok",
        path=str(out_path),
        resolved_url=state.resolved_url,
        content_length=state.content_length,
        sha256=sha256,
    ).to_dict()

    if ctx.mode.verify_sha256 and "sha256" not in result:
        result["sha256"] = HttpDownloadBase.sha256_file(out_path)

    return result


@stable_api
async def async_download_with_resume(
    ctx: AcquireContext,
    url: str,
    out_path: Path,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    semaphore: asyncio.Semaphore | None = None,
) -> dict[str, Any]:
    """Download a file via async HTTP with resume support.

    Supports:
    - Resumable downloads using Range headers
    - Retry with exponential backoff for transient errors
    - SHA-256 verification
    - Size verification
    - Redirect chain validation
    - Bounded concurrency via semaphore

    This function automatically uses either aiohttp or httpx depending
    on what is installed, preferring aiohttp.

    Args:
        ctx: Acquire context with configuration
        url: URL to download from
        out_path: Output file path
        expected_size: Optional expected file size for verification
        expected_sha256: Optional expected SHA-256 hash for verification
        semaphore: Optional semaphore for concurrency control

    Returns:
        Result dictionary with status and metadata

    Raises:
        RuntimeError: If no async HTTP library is available
    """
    dep_error = _check_async_dependency()
    if dep_error:
        raise RuntimeError(dep_error)

    library = _get_async_library()
    if library == "aiohttp":
        return await _async_download_aiohttp(
            ctx, url, out_path, expected_size, expected_sha256, semaphore
        )
    else:
        return await _async_download_httpx(
            ctx, url, out_path, expected_size, expected_sha256, semaphore
        )


@stable_api
async def handle_http_async_multi(
    ctx: AcquireContext,
    row: dict[str, Any],
    out_dir: Path,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT_DOWNLOADS,
) -> list[dict[str, Any]]:
    """Handle async HTTP download of multiple URLs with bounded concurrency.

    Downloads all URLs specified in the download configuration,
    with support for per-file filenames, SHA-256 verification,
    size limits, and bounded concurrency.

    Args:
        ctx: Acquire context with configuration
        row: Target row with download configuration
        out_dir: Output directory for downloaded files
        max_concurrent: Maximum concurrent downloads (default: 5)

    Returns:
        List of result dictionaries for each downloaded file

    Raises:
        RuntimeError: If no async HTTP library is available
    """
    dep_error = _check_async_dependency()
    if dep_error:
        raise RuntimeError(dep_error)

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
        expected_size_map = {
            str(k): int(v) for k, v in expected_size.items() if v is not None
        }

    # Build download tasks
    download_tasks: list[tuple[int, str, Path, int | None, str | None]] = []

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

        download_tasks.append((idx, url, out_path, size_hint, expected))

    if not download_tasks:
        return results

    # Create semaphore for bounded concurrency
    semaphore = asyncio.Semaphore(max_concurrent)

    async def download_with_tracking(
        task_idx: int,
        task_url: str,
        task_out_path: Path,
        task_size_hint: int | None,
        task_expected_sha256: str | None,
    ) -> tuple[int, dict[str, Any]]:
        """Download and return result with index for ordering."""
        try:
            result = await async_download_with_resume(
                ctx,
                task_url,
                task_out_path,
                task_size_hint,
                task_expected_sha256,
                semaphore,
            )
            return (task_idx, result)
        except Exception as exc:
            return (
                task_idx,
                {
                    "status": "error",
                    "error": "download_failed",
                    "message": str(exc),
                    "url": task_url,
                },
            )

    # Execute downloads concurrently
    coros = [
        download_with_tracking(idx, url, out_path, size_hint, expected_sha)
        for idx, url, out_path, size_hint, expected_sha in download_tasks
    ]

    download_results = await asyncio.gather(*coros)

    # Sort results by original index and process
    download_results_sorted = sorted(download_results, key=lambda x: x[0])

    for task_idx, result in download_results_sorted:
        # Find the corresponding task info
        task_info = next(t for t in download_tasks if t[0] == task_idx)
        _idx, _url, out_path, _size_hint, _expected = task_info
        filename = out_path.name

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
async def handle_http_async_single(
    ctx: AcquireContext,
    row: dict[str, Any],
    out_dir: Path,
) -> list[dict[str, Any]]:
    """Handle async HTTP download of a single URL.

    Downloads a single URL from the download configuration,
    with SHA-256 verification and size limit support.

    Args:
        ctx: Acquire context with configuration
        row: Target row with download configuration
        out_dir: Output directory for downloaded file

    Returns:
        List containing a single result dictionary

    Raises:
        RuntimeError: If no async HTTP library is available
    """
    dep_error = _check_async_dependency()
    if dep_error:
        raise RuntimeError(dep_error)

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

    filename = download.get("filename") or safe_filename(
        urlparse(url).path.split("/")[-1]
    )
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

    result = await async_download_with_resume(
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
async def handle_http_async(
    ctx: AcquireContext,
    row: dict[str, Any],
    out_dir: Path,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT_DOWNLOADS,
) -> list[dict[str, Any]]:
    """Handle async HTTP download, routing to single or multi handler.

    Automatically determines whether to use single or multi handler
    based on the number of URLs in the configuration.

    Args:
        ctx: Acquire context with configuration
        row: Target row with download configuration
        out_dir: Output directory for downloaded files
        max_concurrent: Maximum concurrent downloads for multi-URL (default: 5)

    Returns:
        List of result dictionaries

    Raises:
        RuntimeError: If no async HTTP library is available
    """
    download = normalize_download(row.get("download", {}) or {})
    urls: list[str] = []
    if download.get("url"):
        urls.append(download["url"])
    urls.extend([u for u in download.get("urls") or [] if u])

    if len(urls) > 1:
        return await handle_http_async_multi(ctx, row, out_dir, max_concurrent)
    return await handle_http_async_single(ctx, row, out_dir)


def resolve_async_http_handler(
    variant: str = "multi",
    max_concurrent: int = DEFAULT_MAX_CONCURRENT_DOWNLOADS,
) -> AsyncStrategyHandler:
    """Resolve the async HTTP handler based on variant.

    Args:
        variant: Either "single" for single-URL downloads or "multi" for multi-URL
        max_concurrent: Maximum concurrent downloads for multi variant

    Returns:
        The appropriate async strategy handler function
    """
    if variant == "single":

        async def single_handler(
            ctx: AcquireContext, row: dict[str, Any], out_dir: Path
        ) -> list[dict[str, Any]]:
            return await handle_http_async_single(ctx, row, out_dir)

        return single_handler

    async def multi_handler(
        ctx: AcquireContext, row: dict[str, Any], out_dir: Path
    ) -> list[dict[str, Any]]:
        return await handle_http_async_multi(ctx, row, out_dir, max_concurrent)

    return multi_handler


@stable_api
def get_async_handler(
    max_concurrent: int = DEFAULT_MAX_CONCURRENT_DOWNLOADS,
) -> AsyncStrategyHandler:
    """Get the async HTTP handler.

    Returns a handler that automatically routes to single or multi
    based on URL count, with bounded concurrency for multi-URL downloads.

    Args:
        max_concurrent: Maximum concurrent downloads (default: 5)

    Returns:
        Async strategy handler function

    Raises:
        RuntimeError: If no async HTTP library is available
    """
    dep_error = _check_async_dependency()
    if dep_error:
        raise RuntimeError(dep_error)

    async def handler(
        ctx: AcquireContext, row: dict[str, Any], out_dir: Path
    ) -> list[dict[str, Any]]:
        return await handle_http_async(ctx, row, out_dir, max_concurrent)

    return handler


def get_multi_handler(
    max_concurrent: int = DEFAULT_MAX_CONCURRENT_DOWNLOADS,
) -> AsyncStrategyHandler:
    """Get the multi-URL async HTTP handler.

    Args:
        max_concurrent: Maximum concurrent downloads (default: 5)

    Returns:
        The handle_http_async_multi function wrapped with max_concurrent
    """

    async def handler(
        ctx: AcquireContext, row: dict[str, Any], out_dir: Path
    ) -> list[dict[str, Any]]:
        return await handle_http_async_multi(ctx, row, out_dir, max_concurrent)

    return handler


def get_single_handler() -> AsyncStrategyHandler:
    """Get the single-URL async HTTP handler.

    Returns:
        The handle_http_async_single function
    """
    return handle_http_async_single


__all__ = [
    "async_download_with_resume",
    "handle_http_async",
    "handle_http_async_multi",
    "handle_http_async_single",
    "get_async_handler",
    "get_multi_handler",
    "get_single_handler",
    "resolve_async_http_handler",
    "is_async_available",
    "AsyncStrategyHandler",
    "DEFAULT_MAX_CONCURRENT_DOWNLOADS",
]
