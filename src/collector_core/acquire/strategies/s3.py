"""S3 acquisition strategy handlers.

This module provides handlers for AWS S3 data acquisition:
- handle_s3_sync: Sync data from S3 buckets using aws s3 sync
- handle_aws_requester_pays: Download from requester-pays S3 buckets
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from collector_core.acquire.context import AcquireContext, StrategyHandler
from collector_core.acquire_limits import build_target_limit_enforcer, resolve_result_bytes
from collector_core.acquire_strategies import normalize_download
from collector_core.stability import stable_api
from collector_core.utils.hash import sha256_file
from collector_core.utils.paths import ensure_dir, safe_filename


@stable_api
def run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
    """Run a shell command and return its output.

    Args:
        cmd: Command and arguments to execute.
        cwd: Working directory for the command.

    Returns:
        Command stdout as a string.

    Raises:
        subprocess.CalledProcessError: If the command fails.
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
def handle_s3_sync(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Sync data from S3 bucket URLs using aws s3 sync.

    Expects download config with:
        urls: List of S3 URLs to sync (s3://bucket/prefix)
        no_sign_request: Optional bool to skip AWS signing
        request_payer: Optional request payer setting
        extra_args: Optional list of additional aws s3 sync arguments

    Args:
        ctx: Acquisition context with limits and mode settings.
        row: Target row with download configuration.
        out_dir: Output directory for synced data.

    Returns:
        List of result dictionaries with status and details.
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    urls = download.get("urls") or []
    if not urls:
        return [{"status": "error", "error": "missing urls"}]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_dir)}]
    ensure_dir(out_dir)
    results: list[dict[str, Any]] = []
    extra_args = download.get("extra_args", []) or []
    for url in urls:
        limit_error = enforcer.start_file(url)
        if limit_error:
            results.append(limit_error)
            break
        limit_error = enforcer.check_remaining_bytes(url)
        if limit_error:
            results.append(limit_error)
            break
        before_bytes = resolve_result_bytes({}, out_dir) or 0
        cmd = ["aws", "s3", "sync", url, str(out_dir)]
        if download.get("no_sign_request"):
            cmd.append("--no-sign-request")
        if download.get("request_payer"):
            cmd += ["--request-payer", str(download.get("request_payer"))]
        cmd += [str(a) for a in extra_args]
        log = run_cmd(cmd)
        after_bytes = resolve_result_bytes({}, out_dir) or before_bytes
        delta_bytes = max(0, after_bytes - before_bytes)
        limit_error = enforcer.record_bytes(delta_bytes, url)
        result = {"status": "ok", "path": str(out_dir), "log": log}
        if limit_error:
            results.append(limit_error)
        else:
            results.append(result)
    return results


@stable_api
def handle_aws_requester_pays(
    ctx: AcquireContext, row: dict[str, Any], out_dir: Path
) -> list[dict[str, Any]]:
    """Download a file from an AWS S3 requester-pays bucket.

    Expects download config with:
        bucket: S3 bucket name
        key: Object key within the bucket
        dest_filename: Optional destination filename (defaults to key basename)
        request_payer: Optional payer setting (defaults to "requester")
        expected_sha256: Optional SHA-256 hash to verify download

    Args:
        ctx: Acquisition context with limits and mode settings.
        row: Target row with download configuration.
        out_dir: Output directory for downloaded file.

    Returns:
        List of result dictionaries with status and details.
    """
    download = normalize_download(row.get("download", {}) or {})
    enforcer = build_target_limit_enforcer(
        target_id=str(row.get("id", "unknown")),
        limit_files=ctx.limits.limit_files,
        max_bytes_per_target=ctx.limits.max_bytes_per_target,
        download=download,
        run_budget=ctx.run_budget,
    )
    bucket = download.get("bucket")
    key = download.get("key")
    if not bucket or not key:
        return [{"status": "error", "error": "missing bucket/key"}]
    dest_filename = download.get("dest_filename") or safe_filename(Path(key).name)
    limit_error = enforcer.start_file(dest_filename)
    if limit_error:
        return [limit_error]
    limit_error = enforcer.check_remaining_bytes(dest_filename)
    if limit_error:
        return [limit_error]
    out_path = out_dir / dest_filename
    if out_path.exists() and not ctx.mode.overwrite:
        result = {"status": "ok", "path": str(out_path), "cached": True}
        size_bytes = resolve_result_bytes(result, out_path)
        limit_error = enforcer.record_bytes(size_bytes, dest_filename)
        return [limit_error] if limit_error else [result]
    if not ctx.mode.execute:
        return [{"status": "noop", "path": str(out_path)}]
    ensure_dir(out_path.parent)
    payer = download.get("request_payer", "requester")
    temp_path = out_path.with_name(f"{out_path.name}.part")
    cmd = [
        "aws",
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        key,
        str(temp_path),
        "--request-payer",
        payer,
    ]
    log = run_cmd(cmd)
    content_length = temp_path.stat().st_size
    sha256 = sha256_file(temp_path)
    # Handle case where sha256_file returns None (should not happen for existing files)
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
                "resolved_url": f"s3://{bucket}/{key}",
                "content_length": content_length,
                "log": log,
            }
        ]
    limit_error = enforcer.record_bytes(content_length, dest_filename)
    if limit_error:
        temp_path.unlink(missing_ok=True)
        return [limit_error]
    temp_path.replace(out_path)
    result = {
        "status": "ok",
        "path": str(out_path),
        "log": log,
        "resolved_url": f"s3://{bucket}/{key}",
        "content_length": content_length,
        "sha256": sha256,
    }
    return [result]


def get_sync_handler() -> StrategyHandler:
    """Get the S3 sync strategy handler.

    Returns:
        The handle_s3_sync function.
    """
    return handle_s3_sync


def get_requester_pays_handler() -> StrategyHandler:
    """Get the AWS requester-pays strategy handler.

    Returns:
        The handle_aws_requester_pays function.
    """
    return handle_aws_requester_pays
