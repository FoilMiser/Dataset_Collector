"""FTP acquisition strategy handlers."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from collector_core.acquire.context import AcquireContext, StrategyHandler
from collector_core.acquire_limits import build_target_limit_enforcer
from collector_core.acquire_strategies import normalize_download
from collector_core.dependencies import _try_import, requires
from collector_core.stability import stable_api
from collector_core.utils.hash import sha256_file
from collector_core.utils.paths import ensure_dir

FTP = _try_import("ftplib", "FTP")

# Pattern to detect unsafe characters in FTP filenames
_UNSAFE_FILENAME_PATTERN = re.compile(r"[\x00-\x1f\x7f]|\.\.|\\/")


def _is_safe_filename(fname: str) -> bool:
    """Validate that a filename from FTP server is safe.

    Rejects filenames containing:
    - Control characters (newlines, carriage returns, null bytes, etc.)
    - Path traversal sequences (..)
    - Absolute path indicators (leading /)
    - Backslashes (Windows path separators)

    Args:
        fname: The filename to validate.

    Returns:
        True if the filename is safe, False otherwise.
    """
    if not fname or not fname.strip():
        return False
    # Reject control characters, path traversal, and absolute paths
    if _UNSAFE_FILENAME_PATTERN.search(fname):
        return False
    # Reject absolute paths
    if fname.startswith("/") or fname.startswith("\\"):
        return False
    # Reject filenames that are just dots
    if fname.strip(".") == "":
        return False
    return True


@stable_api
def handle_ftp(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    """Handle FTP download strategy.

    Downloads files from an FTP server using glob patterns.

    Args:
        ctx: The acquire context with configuration and limits.
        row: The target row containing download configuration with:
            - base_url: FTP URL to connect to (e.g., ftp://example.com/path)
            - globs: List of glob patterns to match files (default: ["*"])
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
    base = download.get("base_url")
    globs = download.get("globs", ["*"])
    missing = requires("ftplib", FTP, install="use a standard Python build that includes ftplib")
    if missing:
        return [{"status": "error", "error": missing}]
    if not base:
        return [{"status": "error", "error": "missing base_url"}]
    url = urlparse(base)
    results: list[dict[str, Any]] = []
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    with FTP(url.hostname) as ftp:
        ftp.login()
        ftp.cwd(url.path)
        for g in globs:
            files = ftp.nlst(g)
            for fname in files:
                # P0.1: Validate filename to prevent command injection
                if not _is_safe_filename(fname):
                    results.append({
                        "status": "error",
                        "error": f"Unsafe filename from FTP server: {fname!r}",
                    })
                    continue
                limit_error = enforcer.start_file(fname)
                if limit_error:
                    results.append(limit_error)
                    return results
                limit_error = enforcer.check_remaining_bytes(fname)
                if limit_error:
                    results.append(limit_error)
                    return results
                local = out_dir / fname
                ensure_dir(local.parent)
                temp_path = local.with_name(f"{local.name}.part")
                with temp_path.open("wb") as f:
                    ftp.retrbinary(f"RETR {fname}", f.write)
                content_length = temp_path.stat().st_size
                limit_error = enforcer.check_size_hint(content_length, fname)
                if limit_error:
                    temp_path.unlink(missing_ok=True)
                    results.append(limit_error)
                    continue
                sha256 = sha256_file(temp_path)
                limit_error = enforcer.record_bytes(content_length, fname)
                if limit_error:
                    temp_path.unlink(missing_ok=True)
                    results.append(limit_error)
                    continue
                temp_path.replace(local)
                resolved_url = f"{base.rstrip('/')}/{fname}" if base else fname
                results.append(
                    {
                        "status": "ok",
                        "path": str(local),
                        "resolved_url": resolved_url,
                        "content_length": content_length,
                        "sha256": sha256,
                    }
                )
    return results


def get_handler() -> StrategyHandler:
    """Return the FTP strategy handler function."""
    return handle_ftp
