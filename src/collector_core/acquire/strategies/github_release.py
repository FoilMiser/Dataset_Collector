"""GitHub Releases acquisition strategy handlers.

This module provides the GitHub Releases download strategy for acquiring
release assets from GitHub repositories via their REST API.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from collector_core.__version__ import __version__ as VERSION
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
from collector_core.rate_limit import get_resolver_rate_limiter
from collector_core.stability import stable_api
from collector_core.utils.io import write_json
from collector_core.utils.paths import ensure_dir, safe_filename

# Alias for backwards compatibility
safe_name = safe_filename

requests = _try_import("requests")

logger = logging.getLogger(__name__)


@stable_api
def make_github_release_handler(user_agent: str) -> StrategyHandler:
    """Create a GitHub release handler with the specified user agent.

    This factory function creates a strategy handler configured with a
    custom User-Agent string for GitHub API requests. The handler fetches
    release metadata and downloads all associated assets.

    Args:
        user_agent: The base user agent string to use for API requests.
            The version will be appended automatically.

    Returns:
        A StrategyHandler function that can process GitHub release downloads.
    """

    def _handle_github_release(
        ctx: AcquireContext, row: dict[str, Any], out_dir: Path
    ) -> list[dict[str, Any]]:
        """Handle GitHub release asset downloads via the GitHub REST API.

        This handler fetches metadata for a GitHub release and downloads all
        associated assets. It supports specifying releases by:
        - Owner and repo with optional tag or release_id
        - Repository string in "owner/repo" format

        Authentication is supported via:
        - `github_token` in the download config
        - `GITHUB_TOKEN` environment variable
        - `~/.github_token` file

        Args:
            ctx: The acquisition context containing configuration and limits.
            row: The target row containing download configuration.
            out_dir: The output directory for downloaded files.

        Returns:
            A list of result dictionaries, one per asset downloaded or attempted.
            Each result contains status information and file metadata.
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
        owner = download.get("owner")
        repo = download.get("repo") or download.get("repository")
        if repo and "/" in repo and not owner:
            owner, repo = repo.split("/", 1)
        tag = download.get("tag")
        release_id = download.get("release_id")
        if not owner or not repo:
            return [{"status": "error", "error": "missing owner/repo"}]
        headers = {"User-Agent": f"{user_agent}/{VERSION}"}
        # P0.5: Warn if github_token is set in config (should use env var instead)
        if download.get("github_token"):
            logger.warning(
                "github_token in config is deprecated; use GITHUB_TOKEN env var or gh auth"
            )
        # Only use env var for token - file-based tokens removed for security
        token = os.environ.get("GITHUB_TOKEN", "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        base = f"https://api.github.com/repos/{owner}/{repo}/releases"
        if release_id:
            url = f"{base}/{release_id}"
        elif tag:
            url = f"{base}/tags/{tag}"
        else:
            url = f"{base}/latest"
        if not ctx.mode.execute:
            return [{"status": "noop", "release_url": url, "path": str(out_dir)}]

        # Get rate limiter from config - GitHub uses 403 for rate limits
        rate_limiter, rate_config = get_resolver_rate_limiter(ctx.cfg, "github")

        def _fetch() -> requests.Response:
            # Acquire rate limit token before API request
            if rate_limiter:
                rate_limiter.acquire()
            resp = requests.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            return resp

        resp = _with_retries(
            _fetch,
            max_attempts=ctx.retry.max_attempts,
            backoff_base=ctx.retry.backoff_base,
            backoff_max=ctx.retry.backoff_max,
            retry_on_429=rate_config.retry_on_429,
            retry_on_403=rate_config.retry_on_403,  # GitHub uses 403 for rate limits
        )
        # P1.2C: Handle JSON decode errors from API response
        try:
            meta = resp.json()
        except json.JSONDecodeError as e:
            return [{"status": "error", "error": f"Invalid JSON from GitHub API: {e}"}]
        assets = meta.get("assets", []) or []
        results: list[dict[str, Any]] = []
        ensure_dir(out_dir)
        for idx, asset in enumerate(assets):
            download_url = asset.get("browser_download_url") or asset.get("url")
            if not download_url:
                results.append(
                    {"status": "error", "error": "missing_download_url", "asset": asset.get("name")}
                )
                continue
            fname = safe_name(asset.get("name") or f"{repo}_asset_{idx}")
            limit_error = enforcer.start_file(fname)
            if limit_error:
                results.append(limit_error)
                break
            limit_error = enforcer.check_remaining_bytes(fname)
            if limit_error:
                results.append(limit_error)
                break
            size_hint = asset.get("size")
            limit_error = enforcer.check_size_hint(
                int(size_hint) if size_hint is not None else None, fname
            )
            if limit_error:
                results.append(limit_error)
                continue
            out_path = out_dir / fname
            result = _http_download_with_resume(ctx, download_url, out_path, size_hint)
            size_bytes = resolve_result_bytes(result, out_path)
            limit_error = enforcer.record_bytes(size_bytes, fname)
            if limit_error:
                if result.get("status") == "ok" and not result.get("cached"):
                    cleanup_path(out_path)
                results.append(limit_error)
            else:
                results.append(result)
        write_json(out_dir / "github_release.json", meta)
        return results

    return _handle_github_release


def resolve_handler(repo: str) -> StrategyHandler:
    """Create a GitHub release handler using the specified repo as user agent.

    This is a convenience function that wraps make_github_release_handler
    to provide a simple interface for creating handlers.

    Args:
        repo: The repository identifier to use as the base user agent string.
            Typically in the format "owner/repo" or just a project name.

    Returns:
        A StrategyHandler function configured for GitHub release downloads.
    """
    return make_github_release_handler(repo)
