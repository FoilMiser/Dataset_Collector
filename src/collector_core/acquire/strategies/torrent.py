"""Torrent acquisition strategy handlers.

Torrent/magnet download via aria2c.
"""

from __future__ import annotations

import subprocess
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
from collector_core.utils.paths import ensure_dir


@stable_api
def run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
    """Run a command and return its output.

    Args:
        cmd: Command and arguments to execute.
        cwd: Optional working directory.

    Returns:
        Decoded stdout output from the command.

    Raises:
        subprocess.CalledProcessError: If command exits with non-zero status.
    """
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return p.stdout.decode("utf-8", errors="ignore")


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
    except Exception as e:
        return [{"status": "error", "error": repr(e)}]


def get_handler() -> StrategyHandler:
    """Return the torrent strategy handler.

    Returns:
        The handle_torrent function.
    """
    return handle_torrent
