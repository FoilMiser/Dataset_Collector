"""Torrent acquisition strategy handlers.

Torrent/magnet download via aria2c.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

from collector_core.acquire.context import AcquireContext, StrategyHandler
from collector_core.acquire_limits import (
    build_target_limit_enforcer,
    cleanup_path,
    resolve_result_bytes,
)
from collector_core.stability import stable_api
from collector_core.utils.download import normalize_download
from collector_core.utils.paths import ensure_dir
from collector_core.utils.subprocess import run_cmd

# Pattern for valid magnet URIs (BTIHv1: 40 hex chars, BTIHv2: 64 hex chars)
_MAGNET_PATTERN = re.compile(
    r"^magnet:\?xt=urn:btih:[a-fA-F0-9]{40}([a-fA-F0-9]{24})?(&.*)?$"
)

# Pattern for valid .torrent file paths (local files only)
_TORRENT_FILE_PATTERN = re.compile(r"^[a-zA-Z0-9_./-]+\.torrent$")

# Shell metacharacters that should never appear in magnet/torrent links
_SHELL_METACHAR_PATTERN = re.compile(r"[;&|`$(){}[\]<>!#\n\r]")


def _is_valid_magnet(link: str) -> bool:
    """Validate that a magnet link or torrent path is safe.

    Validates magnet links have proper format:
    - Must start with "magnet:?xt=urn:btih:"
    - Must contain a 40-char (v1) or 64-char (v2) hex infohash
    - Must not contain shell metacharacters

    For .torrent files:
    - Must be a simple path ending in .torrent
    - Must not contain shell metacharacters

    Args:
        link: The magnet URI or torrent file path to validate.

    Returns:
        True if the link is safe, False otherwise.
    """
    if not link or not isinstance(link, str):
        return False

    # Reject any shell metacharacters
    if _SHELL_METACHAR_PATTERN.search(link):
        return False

    # Check if it's a magnet link
    if link.startswith("magnet:"):
        return bool(_MAGNET_PATTERN.match(link))

    # Check if it's a .torrent file path
    if link.endswith(".torrent"):
        return bool(_TORRENT_FILE_PATTERN.match(link))

    return False


@stable_api
def handle_torrent(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Handle torrent/magnet download via aria2c.

    Args:
        ctx: Acquire context with configuration and limits.
        row: Target row containing download configuration.
        out_dir: Output directory for downloaded files.

    Returns:
        List of result dictionaries with status and path information.
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    magnet = download.get("magnet") or download.get("torrent")
    if not magnet:
        return [{"status": "error", "error": "missing magnet/torrent"}]
    # P0.2: Validate magnet link format to prevent command injection
    if not _is_valid_magnet(magnet):
        return [{"status": "error", "error": f"Invalid magnet link format: {magnet[:50]!r}..."}]
    limit_error = enforcer.start_file(magnet)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(magnet)
    if limit_error:
        return [limit_error]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    ensure_dir(out_dir)
    try:
        log = run_cmd(["aria2c", "--seed-time=0", "-d", str(out_dir), magnet])
        result = {"status": "ok", "path": str(out_dir), "log": log}
        size_bytes = resolve_result_bytes(result, out_dir)
        limit_error = enforcer.record_bytes(size_bytes, magnet)
        if limit_error:
            cleanup_path(out_dir)
            return [limit_error]
        return [result]
    except subprocess.SubprocessError as e:
        return [{"status": "error", "error": repr(e)}]


def get_handler() -> StrategyHandler:
    """Return the torrent strategy handler.

    Returns:
        The handle_torrent function.
    """
    return handle_torrent
