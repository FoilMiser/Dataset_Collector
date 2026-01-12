"""Git acquisition strategy handlers."""

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
from collector_core.stability import stable_api
from collector_core.utils.paths import ensure_dir, safe_filename


def normalize_download(download: dict[str, Any]) -> dict[str, Any]:
    """Normalize download configuration by merging nested config and handling aliases."""
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
def run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
    """Run a shell command and return its stdout output.

    Args:
        cmd: Command and arguments as a list of strings.
        cwd: Optional working directory for the command.

    Returns:
        The stdout output of the command as a string.

    Raises:
        subprocess.CalledProcessError: If the command exits with non-zero status.
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
def handle_git(ctx: AcquireContext, row: dict[str, Any], out_dir: Path) -> list[dict[str, Any]]:
    """Handle git clone acquisition strategy.

    Clones a git repository to the output directory. Supports specifying
    a branch, commit, or tag to checkout after cloning.

    Args:
        ctx: The acquisition context with configuration and limits.
        row: The target row containing download configuration.
        out_dir: The output directory where the repository will be cloned.

    Returns:
        A list containing a single result dictionary with status and metadata.
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    repo = (
        download.get("repo")
        or download.get("repo_url")
        or download.get("url")
        or download.get("url")
    )
    branch = download.get("branch")
    commit = download.get("commit")
    tag = download.get("tag")
    revision = commit or tag
    if not repo:
        return [{"status": "error", "error": "missing repo"}]
    limit_error = enforcer.start_file(repo)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(repo)
    if limit_error:
        return [limit_error]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    if out_dir.exists() and any(out_dir.iterdir()) and not ctx.mode.overwrite:
        git_dir = out_dir / ".git"
        if not git_dir.exists():
            return [{"status": "error", "error": "missing_git_repo", "path": str(out_dir)}]
        if revision:
            if tag:
                run_cmd(["git", "-C", str(out_dir), "fetch", "--tags", "--force"])
            else:
                run_cmd(["git", "-C", str(out_dir), "fetch", "--all", "--prune"])
            run_cmd(["git", "-C", str(out_dir), "checkout", revision])
        resolved = run_cmd(["git", "-C", str(out_dir), "rev-parse", "HEAD"]).strip()
        result = {"status": "ok", "path": str(out_dir), "cached": True, "git_commit": resolved}
        if revision:
            result["git_revision"] = revision
        size_bytes = resolve_result_bytes(result, out_dir)
        limit_error = enforcer.record_bytes(size_bytes, repo)
        return [limit_error] if limit_error else [result]
    ensure_dir(out_dir)
    cmd = ["git", "clone"]
    if branch and not revision:
        cmd += ["-b", branch]
    cmd += [repo, str(out_dir)]
    log = run_cmd(cmd)
    if revision:
        run_cmd(["git", "-C", str(out_dir), "checkout", revision])
    resolved = run_cmd(["git", "-C", str(out_dir), "rev-parse", "HEAD"]).strip()
    result = {"status": "ok", "path": str(out_dir), "log": log, "git_commit": resolved}
    if revision:
        result["git_revision"] = revision
    size_bytes = resolve_result_bytes(result, out_dir)
    limit_error = enforcer.record_bytes(size_bytes, repo)
    if limit_error:
        cleanup_path(out_dir)
        return [limit_error]
    return [result]


def get_handler() -> StrategyHandler:
    """Return the git strategy handler function."""
    return handle_git
