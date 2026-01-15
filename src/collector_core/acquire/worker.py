"""Acquire worker module for high-level acquisition orchestration.

This module contains the worker functions for running acquisition pipelines:
- resolve_output_dir: Resolve the output directory for a target
- write_done_marker: Write completion marker for acquired target
- run_target: Run acquisition for a single target
- load_config: Load targets configuration
- load_roots: Load and resolve acquisition roots
- run_acquire_worker: Main worker entrypoint

Usage:
    from collector_core.acquire.worker import run_acquire_worker, run_target

    # Run the full worker (CLI entrypoint)
    run_acquire_worker(
        defaults=RootsDefaults(...),
        targets_yaml_label="targets.yaml",
        strategy_handlers=build_default_handlers(),
    )

    # Or run a single target
    result = run_target(ctx, bucket, row, strategy_handlers)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.acquire.context import (
    AcquireContext,
    Limits,
    PostProcessor,
    RetryConfig,
    Roots,
    RootsDefaults,
    RunMode,
    StrategyHandler,
    _build_internal_mirror_allowlist,
    _normalize_internal_mirror_allowlist,
)
from collector_core.acquire_limits import build_run_budget, resolve_result_bytes
from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.checks.runner import generate_run_id, run_checks_for_target
from collector_core.config_validator import read_yaml
from collector_core.dataset_root import ensure_data_root_allowed, resolve_dataset_root
from collector_core.logging_config import LogContext, add_logging_args, configure_logging
from collector_core.stability import stable_api
from collector_core.utils.io import read_jsonl_list, write_json
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir, safe_filename

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# License pool mapping for output directory resolution
LICENSE_POOL_MAP = {
    "permissive": "permissive",
    "public_domain": "permissive",
    "record_level": "permissive",
    "copyleft": "copyleft",
    "unknown": "quarantine",
    "quarantine": "quarantine",
    "deny": "quarantine",
}


@stable_api
def resolve_license_pool(row: dict[str, Any]) -> str:
    """Resolve the license pool for a target row.

    Args:
        row: Target row dict containing license_profile or license_pool

    Returns:
        License pool string (permissive, copyleft, or quarantine)
    """
    lp = str(row.get("license_profile") or row.get("license_pool") or "quarantine").lower()
    return LICENSE_POOL_MAP.get(lp, "quarantine")


@stable_api
def resolve_output_dir(
    ctx: AcquireContext, bucket: str, pool: str, target_id: str
) -> Path:
    """Resolve the output directory for a target.

    Creates the directory structure: raw_root / bucket / pool / target_id

    Args:
        ctx: Acquire context with roots configuration
        bucket: Bucket name (green, yellow)
        pool: License pool (permissive, copyleft, quarantine)
        target_id: Target identifier

    Returns:
        Path to the output directory (created if needed)
    """
    bucket = (bucket or "yellow").strip().lower()
    pool = (pool or "quarantine").strip().lower()
    out = ctx.roots.raw_root / bucket / pool / safe_filename(target_id)
    ensure_dir(out)
    return out


@stable_api
def write_done_marker(
    ctx: AcquireContext,
    target_id: str,
    bucket: str,
    status: str,
    extra: dict[str, Any] | None = None,
) -> None:
    """Write the acquisition done marker file.

    Creates acquire_done.json in the target's manifest directory with
    status and metadata about the acquisition.

    Args:
        ctx: Acquire context with roots configuration
        target_id: Target identifier
        bucket: Bucket name (green, yellow)
        status: Acquisition status (ok, error, noop)
        extra: Optional extra metadata to include
    """
    marker = ctx.roots.manifests_root / safe_filename(target_id) / "acquire_done.json"
    payload = {
        "target_id": target_id,
        "bucket": bucket,
        "status": status,
        "written_at_utc": utc_now(),
        "version": VERSION,
    }
    payload.update(
        build_artifact_metadata(
            written_at_utc=payload["written_at_utc"],
            git_commit=extra.get("git_commit") if extra else None,
        )
    )
    if extra:
        payload.update(extra)
    write_json(marker, payload)


def _sum_result_bytes(results: list[dict[str, Any]], out_dir: Path) -> int | None:
    """Sum the bytes from acquisition results.

    Args:
        results: List of result dictionaries from handlers
        out_dir: Default output directory

    Returns:
        Total bytes or None if no size information available
    """
    total = 0
    found = False
    for result in results:
        path_value = result.get("path")
        path = Path(path_value) if path_value else out_dir
        size = resolve_result_bytes(result, path)
        if size is not None:
            total += size
            found = True
    return total if found else None


@stable_api
def run_target(
    ctx: AcquireContext,
    bucket: str,
    row: dict[str, Any],
    strategy_handlers: dict[str, StrategyHandler],
    postprocess: PostProcessor | None = None,
) -> dict[str, Any]:
    """Run acquisition for a single target.

    Executes the appropriate strategy handler for the target's download
    configuration, writes manifests, and optionally runs postprocessing.

    Args:
        ctx: Acquire context with configuration and limits
        bucket: Bucket name (green, yellow)
        row: Target row dict with id, download config, etc.
        strategy_handlers: Dict mapping strategy names to handler functions
        postprocess: Optional postprocessor function

    Returns:
        Result dict with id, status, bucket, license_pool, strategy
    """
    tid = row["id"]
    pool = resolve_license_pool(row)
    content_checks = row.get("content_checks") or []
    if not isinstance(content_checks, list):
        content_checks = [str(content_checks)]
    strat = (row.get("download", {}) or {}).get("strategy", "none")
    domain = (
        row.get("routing_domain")
        or (row.get("routing") or {}).get("domain")
        or row.get("domain")
    )
    out_dir = resolve_output_dir(ctx, bucket, pool, tid)
    start_time = time.monotonic()
    manifest: dict[str, Any] = {
        "id": tid,
        "name": row.get("name", tid),
        "bucket": bucket,
        "license_pool": pool,
        "strategy": strat,
        "started_at_utc": utc_now(),
        "output_dir": str(out_dir),
        "results": [],
    }

    error_types: list[str] = []
    handler = strategy_handlers.get(strat)
    with LogContext(
        run_id=ctx.checks_run_id,
        domain=domain,
        target_id=tid,
        strategy=strat,
    ):
        logger.info("Acquire target started.")
        if not handler or strat in {"none", ""}:
            manifest["results"] = [{"status": "noop", "reason": f"unsupported: {strat}"}]
        else:
            try:
                manifest["results"] = handler(ctx, row, out_dir)
                if not manifest["results"]:
                    manifest["results"] = [
                        {"status": "failed", "reason": "handler_returned_no_results"}
                    ]
            except Exception as e:
                error_types.append(type(e).__name__)
                manifest["results"] = [{"status": "error", "error": repr(e)}]

        post_processors: dict[str, Any] | None = None
        if postprocess:
            post_processors = postprocess(ctx, row, out_dir, bucket, manifest)
            if post_processors:
                manifest["post_processors"] = post_processors

        manifest["finished_at_utc"] = utc_now()
        git_info: dict[str, Any] | None = None
        for result in manifest["results"]:
            if result.get("git_commit"):
                git_info = {"git_commit": result["git_commit"]}
                if result.get("git_revision"):
                    git_info["git_revision"] = result["git_revision"]
                break
        if git_info:
            manifest.update(git_info)
        manifest.update(
            build_artifact_metadata(
                written_at_utc=manifest["finished_at_utc"],
                git_commit=git_info.get("git_commit") if git_info else None,
            )
        )
        write_json(out_dir / "download_manifest.json", manifest)

        results = manifest["results"]
        if any(r.get("status") == "ok" for r in results):
            status = "ok"
        elif results:
            status = results[0].get("status", "error")
        else:
            status = "error"
        if post_processors:
            for proc in post_processors.values():
                if isinstance(proc, dict) and proc.get("status") not in {"ok", "noop"}:
                    status = proc.get("status", status)
        if ctx.mode.execute:
            write_done_marker(ctx, tid, bucket, status, git_info)
        run_checks_for_target(
            content_checks=content_checks,
            ledger_root=ctx.roots.ledger_root,
            run_id=ctx.checks_run_id,
            target_id=tid,
            stage="acquire",
            row=row,
            extra={"bucket": bucket, "status": status},
        )
        for result in manifest["results"]:
            err = result.get("error") or result.get("reason")
            if err:
                error_types.append(str(err))
        error_types = sorted(set(error_types))
        duration_ms = (time.monotonic() - start_time) * 1000
        bytes_total = _sum_result_bytes(manifest["results"], out_dir)
        with LogContext(bytes=bytes_total, duration_ms=duration_ms, error_types=error_types):
            if status not in {"ok", "noop"}:
                logger.warning("Acquire target finished with errors.")
            else:
                logger.info("Acquire target finished.")
        return {
            "id": tid,
            "status": status,
            "bucket": bucket,
            "license_pool": pool,
            "strategy": strat,
        }


@stable_api
def load_config(targets_path: Path | None) -> dict[str, Any]:
    """Load targets configuration from YAML file.

    Args:
        targets_path: Path to targets YAML file, or None

    Returns:
        Configuration dict (empty if path is None or doesn't exist)
    """
    cfg: dict[str, Any] = {}
    if targets_path and targets_path.exists():
        cfg = read_yaml(targets_path, schema_name="targets") or {}
    return cfg


@stable_api
def load_roots(
    cfg: dict[str, Any], overrides: argparse.Namespace, defaults: RootsDefaults
) -> Roots:
    """Load and resolve acquisition roots from config and overrides.

    Resolves roots in priority order:
    1. Command-line overrides
    2. Config file globals
    3. Dataset root (if set)
    4. Defaults

    Args:
        cfg: Configuration dict (from load_config)
        overrides: Namespace with command-line overrides
        defaults: Default roots configuration

    Returns:
        Resolved Roots dataclass

    Raises:
        ValueError: If attempting to use /data without --allow-data-root
    """
    allow_data_root = bool(getattr(overrides, "allow_data_root", False))
    dataset_root = resolve_dataset_root(getattr(overrides, "dataset_root", None))
    if dataset_root:
        defaults = RootsDefaults(
            raw_root=str(dataset_root / "raw"),
            manifests_root=str(dataset_root / "_manifests"),
            ledger_root=str(dataset_root / "_ledger"),
            logs_root=str(dataset_root / "_logs"),
        )
    g = cfg.get("globals", {}) or {}
    raw_root = Path(
        getattr(overrides, "raw_root", None) or g.get("raw_root", defaults.raw_root)
    )
    manifests_root = Path(
        getattr(overrides, "manifests_root", None)
        or g.get("manifests_root", defaults.manifests_root)
    )
    ledger_root = Path(
        getattr(overrides, "ledger_root", None) or g.get("ledger_root", defaults.ledger_root)
    )
    logs_root = Path(
        getattr(overrides, "logs_root", None) or g.get("logs_root", defaults.logs_root)
    )
    roots = Roots(
        raw_root=raw_root.expanduser().resolve(),
        manifests_root=manifests_root.expanduser().resolve(),
        ledger_root=ledger_root.expanduser().resolve(),
        logs_root=logs_root.expanduser().resolve(),
    )
    ensure_data_root_allowed(
        [roots.raw_root, roots.manifests_root, roots.ledger_root, roots.logs_root],
        allow_data_root,
    )
    return roots


@stable_api
def run_acquire_worker(
    *,
    defaults: RootsDefaults,
    targets_yaml_label: str,
    strategy_handlers: dict[str, StrategyHandler],
    postprocess: PostProcessor | None = None,
) -> None:
    """Main acquire worker entrypoint.

    Parses command-line arguments, loads configuration, and runs acquisition
    for all targets in the queue. Supports parallel execution with threading.

    Args:
        defaults: Default roots configuration
        targets_yaml_label: Label for targets YAML file (for help text)
        strategy_handlers: Dict mapping strategy names to handler functions
        postprocess: Optional postprocessor function

    Raises:
        SystemExit: With code 1 if --strict and any target failed
    """
    ap = argparse.ArgumentParser(description=f"Acquire Worker v{VERSION}")
    ap.add_argument("--queue", required=True, help="Queue JSONL emitted by pipeline_driver.py")
    ap.add_argument(
        "--targets", default=None, help=f"Path to {targets_yaml_label} for roots"
    )
    # P2.4: Deprecated alias for --targets
    ap.add_argument(
        "--targets-yaml", default=None, dest="targets_yaml_deprecated",
        help="DEPRECATED: Use --targets instead"
    )
    ap.add_argument(
        "--bucket", required=True, choices=["green", "yellow"], help="Bucket being processed"
    )
    ap.add_argument(
        "--dataset-root",
        default=None,
        help="Override dataset root (raw/_manifests/_ledger/_logs)",
    )
    ap.add_argument("--raw-root", default=None, help="Override raw root")
    ap.add_argument("--manifests-root", default=None, help="Override manifests root")
    ap.add_argument("--ledger-root", default=None, help="Override ledger root")
    ap.add_argument("--logs-root", default=None, help="Override logs root")
    ap.add_argument(
        "--allow-data-root",
        action="store_true",
        help="Allow /data defaults for outputs (default: disabled).",
    )
    ap.add_argument("--execute", action="store_true", help="Perform downloads")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    ap.add_argument(
        "--verify-sha256", action="store_true", help="Compute sha256 for http downloads"
    )
    ap.add_argument("--verify-zenodo-md5", action="store_true", help="Verify Zenodo md5")
    ap.add_argument(
        "--allow-non-global-download-hosts",
        action="store_true",
        help=(
            "Allow downloads from non-global IPs "
            "(private/loopback/link-local/multicast/reserved/unspecified)."
        ),
    )
    ap.add_argument(
        "--internal-mirror-allowlist",
        action="append",
        default=None,
        help=(
            "Allow internal mirrors by hostname or IP/CIDR (repeatable or comma-separated). "
            "Use sparingly to permit private mirrors."
        ),
    )
    ap.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume partial downloads (default: resume).",
    )
    ap.add_argument("--limit-targets", type=int, default=None)
    ap.add_argument("--limit-files", type=int, default=None)
    ap.add_argument("--max-bytes-per-target", type=int, default=None)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--retry-max", type=int, default=3)
    ap.add_argument("--retry-backoff", type=float, default=2.0)
    ap.add_argument("--strict", "--fail-on-error", dest="strict", action="store_true")
    add_logging_args(ap)
    args = ap.parse_args()
    configure_logging(level=args.log_level, fmt=args.log_format)

    queue_path = Path(args.queue).expanduser().resolve()
    rows = read_jsonl_list(queue_path)

    # P2.4: Handle deprecated --targets-yaml with warning
    targets_arg = args.targets
    if args.targets_yaml_deprecated:
        import warnings
        warnings.warn(
            "--targets-yaml is deprecated; use --targets instead. "
            "This argument will be removed in v4.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        if not targets_arg:
            targets_arg = args.targets_yaml_deprecated

    targets_path = Path(targets_arg).expanduser().resolve() if targets_arg else None
    cfg = load_config(targets_path)
    roots = load_roots(cfg, args, defaults)
    ensure_dir(roots.logs_root)
    ensure_dir(roots.ledger_root)
    globals_cfg = cfg.get("globals", {}) or {}
    cfg_allowlist = _normalize_internal_mirror_allowlist(
        globals_cfg.get("internal_mirror_allowlist")
    )
    arg_allowlist: list[str] = []
    for entry in args.internal_mirror_allowlist or []:
        arg_allowlist.extend(_normalize_internal_mirror_allowlist(entry))
    internal_mirror_allowlist = _build_internal_mirror_allowlist(
        sorted(set(cfg_allowlist + arg_allowlist))
    )
    run_budget = build_run_budget(globals_cfg.get("run_byte_budget"))

    ctx = AcquireContext(
        roots=roots,
        limits=Limits(args.limit_targets, args.limit_files, args.max_bytes_per_target),
        mode=RunMode(
            args.execute,
            args.overwrite,
            args.verify_sha256,
            args.verify_zenodo_md5,
            args.resume,
            max(1, args.workers),
        ),
        retry=RetryConfig(args.retry_max, args.retry_backoff),
        run_budget=run_budget,
        allow_non_global_download_hosts=args.allow_non_global_download_hosts,
        internal_mirror_allowlist=internal_mirror_allowlist,
        cfg=cfg,
        checks_run_id=generate_run_id("acquire"),
    )

    if ctx.limits.limit_targets:
        rows = rows[: ctx.limits.limit_targets]
    rows = [r for r in rows if r.get("enabled", True) and r.get("id")]

    summary: dict[str, Any] = {
        "checks_run_id": ctx.checks_run_id,
        "run_at_utc": utc_now(),
        "queue": str(queue_path),
        "bucket": args.bucket,
        "execute": ctx.mode.execute,
        "results": [],
    }
    summary.update(build_artifact_metadata(written_at_utc=summary["run_at_utc"]))

    if ctx.mode.workers > 1 and ctx.mode.execute:
        with ThreadPoolExecutor(max_workers=ctx.mode.workers) as ex:
            results_by_index: list[dict[str, Any] | None] = [None] * len(rows)
            futures: dict[object, tuple[int, dict[str, Any]]] = {}
            row_iter = iter(enumerate(rows))

            def submit_next() -> bool:
                if ctx.run_budget and ctx.run_budget.exhausted():
                    return False
                try:
                    idx, row = next(row_iter)
                except StopIteration:
                    return False
                fut = ex.submit(
                    run_target, ctx, args.bucket, row, strategy_handlers, postprocess
                )
                futures[fut] = (idx, row)
                return True

            while len(futures) < ctx.mode.workers and submit_next():
                continue
            while futures:
                for fut in as_completed(futures):
                    idx, row = futures.pop(fut)
                    try:
                        res = fut.result()
                    except Exception as e:
                        res = {"id": row.get("id"), "status": "error", "error": repr(e)}
                    results_by_index[idx] = res
                    while len(futures) < ctx.mode.workers and submit_next():
                        continue
                    break
            summary["results"] = [result for result in results_by_index if result is not None]
    else:
        for row in rows:
            if ctx.run_budget and ctx.run_budget.exhausted():
                break
            res = run_target(ctx, args.bucket, row, strategy_handlers, postprocess)
            summary["results"].append(res)

    status_counts = Counter(result.get("status") or "unknown" for result in summary["results"])
    summary["counts"] = {"total": len(summary["results"]), **dict(status_counts)}
    summary["failed_targets"] = [
        {"id": result.get("id", "unknown"), "error": result.get("error", "unknown")}
        for result in summary["results"]
        if result.get("status") == "error"
    ]

    write_json(roots.logs_root / f"acquire_summary_{args.bucket}.json", summary)
    if (
        ctx.mode.execute
        and args.strict
        and any(r.get("status") == "error" for r in summary["results"])
    ):
        sys.exit(1)


__all__ = [
    "LICENSE_POOL_MAP",
    "resolve_license_pool",
    "resolve_output_dir",
    "write_done_marker",
    "run_target",
    "load_config",
    "load_roots",
    "run_acquire_worker",
]
