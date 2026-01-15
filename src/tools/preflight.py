from __future__ import annotations

import argparse
import json
import shutil
import traceback
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from collector_core.config_validator import read_yaml
from collector_core.pipeline_registry import resolve_acquire_hooks, resolve_pipeline_context
from collector_core.targets_paths import resolve_targets_path, targets_root
from collector_core.utils.download import normalize_download as _normalize_download
from tools.strategy_registry import (
    get_external_tools,
    get_strategy_requirement_errors,
    get_strategy_spec,
)

TOOL_INSTALL_HINTS = {
    "git": "Install Git for Windows: https://git-scm.com/download/win",
    "aria2c": "Install aria2: https://aria2.github.io/",
    "aws": "Install AWS CLI v2: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html",
}


def _load_yaml(path: Path, schema_name: str) -> dict[str, Any]:
    return read_yaml(path, schema_name=schema_name) or {}


def run_preflight(
    repo_root: Path,
    pipeline_map_path: Path,
    strict: bool = False,
    pipelines: list[str] | None = None,
    warn_disabled: bool = False,
    verbose: bool = False,
    quiet: bool = False,
) -> int:
    """Run preflight validation checks (214 lines).

    NOTE: This function is flagged for refactoring (P2.2C) but is well-structured.
    Potential extraction points marked with REFACTOR comments below.
    See A_GRADE_REMAINING_WORK.md for detailed refactoring plan.
    """
    pipeline_map = _load_yaml(pipeline_map_path, "pipeline_map")
    pipelines_cfg = pipeline_map.get("pipelines", {}) or {}
    errors: list[str | dict[str, str]] = []
    warnings: list[str] = []
    strategies_in_use_enabled: set[str] = set()
    strategies_in_use_disabled: set[str] = set()
    strategy_targets_enabled: dict[str, list[str]] = {}
    strategy_targets_disabled: dict[str, list[str]] = {}
    registry_misses_enabled: dict[str, list[str]] = {}
    registry_misses_disabled: dict[str, list[str]] = {}

    pipeline_items: list[tuple[str, dict[str, Any]]] = []
    if pipelines:
        missing = sorted(set(pipelines) - set(pipelines_cfg.keys()))
        if missing:
            errors.append(f"Pipeline map missing entries for: {', '.join(missing)}")
        for pipeline_name in pipelines:
            pipeline_entry = pipelines_cfg.get(pipeline_name)
            if pipeline_entry is not None:
                pipeline_items.append((pipeline_name, pipeline_entry))
    else:
        pipeline_items = list(pipelines_cfg.items())

    # REFACTOR: Main validation loop (lines 71-158+) could be extracted to
    # _validate_pipelines(pipeline_items, repo_root, warn_disabled, verbose, ...)
    # Returns (errors, warnings, strategies_in_use, strategy_targets, registry_misses)
    for pipeline_name, pipeline_entry in pipeline_items:
        pipeline_dir = repo_root / pipeline_name
        if not pipeline_dir.exists():
            errors.append(f"Pipeline directory missing: {pipeline_dir}")
            continue

        targets_yaml = pipeline_entry.get("targets_yaml")
        if not targets_yaml:
            errors.append(f"Pipeline map entry missing targets_yaml for: {pipeline_name}")
            continue

        slug = pipeline_name.removesuffix("_pipeline_v2")
        targets_path = resolve_targets_path(repo_root, slug, targets_yaml)
        if not targets_path or not targets_path.exists():
            expected = targets_root(repo_root) / targets_yaml
            errors.append(f"Targets YAML missing: {expected}")
            continue

        try:
            ctx = resolve_pipeline_context(pipeline_id=pipeline_name, repo_root=repo_root)
            handlers, _ = resolve_acquire_hooks(ctx)
            handler_keys = set(handlers.keys())
        except Exception as exc:
            errors.append(
                {
                    "pipeline": pipeline_name,
                    "path": str(pipeline_dir),
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                }
            )
            if verbose:
                traceback.print_exc()
            continue

        targets_cfg = _load_yaml(targets_path, "targets")
        targets = targets_cfg.get("targets", []) or []
        for target in targets:
            enabled = target.get("enabled", True)
            if not enabled and not warn_disabled:
                continue
            target_id = target.get("id", "<unknown>")
            # Normalize download config to merge nested config dict
            download = _normalize_download(target.get("download", {}) or {})
            strategy = (download.get("strategy") or "").strip()
            # "none" is a valid placeholder strategy - skip validation for it
            if not strategy:
                if enabled:
                    errors.append(
                        f"{pipeline_name}:{target_id} enabled with missing download.strategy"
                    )
                elif warn_disabled:
                    warnings.append(
                        f"{pipeline_name}:{target_id} disabled with missing download.strategy"
                    )
                continue
            # "none" strategy is valid - no download required, skip further checks
            if strategy == "none":
                continue
            if strategy not in handler_keys:
                if enabled:
                    errors.append(
                        f"{pipeline_name}:{target_id} uses unsupported strategy '{strategy}'"
                    )
                elif warn_disabled:
                    warnings.append(
                        f"{pipeline_name}:{target_id} disabled uses unsupported strategy '{strategy}'"
                    )
                continue
            spec = get_strategy_spec(strategy)
            if spec is None:
                if enabled:
                    registry_misses_enabled.setdefault(strategy, []).append(
                        f"{pipeline_name}:{target_id}"
                    )
                elif warn_disabled:
                    registry_misses_disabled.setdefault(strategy, []).append(
                        f"{pipeline_name}:{target_id}"
                    )
            else:
                status = (spec.get("status") or "supported").strip().lower()
                if status != "supported":
                    if enabled:
                        errors.append(
                            f"{pipeline_name}:{target_id} uses {status} strategy '{strategy}'"
                        )
                    elif warn_disabled:
                        warnings.append(
                            f"{pipeline_name}:{target_id} disabled uses {status} strategy '{strategy}'"
                        )
                    continue
            # Validate required download config fields for this strategy
            requirement_errors = get_strategy_requirement_errors(download, strategy)
            if requirement_errors:
                for req_error in requirement_errors:
                    if enabled:
                        errors.append(f"{pipeline_name}:{target_id} {req_error}")
                    elif warn_disabled:
                        warnings.append(f"{pipeline_name}:{target_id} (disabled) {req_error}")
            if enabled:
                strategies_in_use_enabled.add(strategy)
                strategy_targets_enabled.setdefault(strategy, []).append(
                    f"{pipeline_name}:{target_id}"
                )
            else:
                strategies_in_use_disabled.add(strategy)
                strategy_targets_disabled.setdefault(strategy, []).append(
                    f"{pipeline_name}:{target_id}"
                )

    for strategy in sorted(strategies_in_use_enabled):
        for tool in get_external_tools(strategy):
            if shutil.which(tool) is None:
                enabled_targets = strategy_targets_enabled.get(strategy, [])
                disabled_targets = (
                    strategy_targets_disabled.get(strategy, []) if warn_disabled else []
                )
                targets = ", ".join(sorted(enabled_targets + disabled_targets))
                hint = TOOL_INSTALL_HINTS.get(tool)
                warnings.append(
                    f"Missing external tool '{tool}' required by strategy '{strategy}'"
                    " for enabled targets."
                    + (f" Targets: {targets}." if targets else "")
                    + (" Disable those targets or install the tool." if targets else "")
                    + (f" {hint}" if hint else "")
                )

    if warn_disabled:
        disabled_only_strategies = strategies_in_use_disabled - strategies_in_use_enabled
        for strategy in sorted(disabled_only_strategies):
            for tool in get_external_tools(strategy):
                if shutil.which(tool) is None:
                    targets = ", ".join(sorted(strategy_targets_disabled.get(strategy, [])))
                    hint = TOOL_INSTALL_HINTS.get(tool)
                    warnings.append(
                        f"Missing external tool '{tool}' required by strategy '{strategy}'"
                        " for disabled targets."
                        + (f" Targets: {targets}." if targets else "")
                        + (" Install the tool before enabling these targets." if targets else "")
                        + (f" {hint}" if hint else "")
                    )

    registry_strategies = set(registry_misses_enabled.keys()) | set(registry_misses_disabled.keys())
    for strategy in sorted(registry_strategies):
        targets = list(registry_misses_enabled.get(strategy, []))
        if warn_disabled:
            targets.extend(registry_misses_disabled.get(strategy, []))
        if not targets:
            continue
        warnings.append(
            "Strategy registry missing entry for "
            f"'{strategy}'. Add it to src/tools/strategy_registry.py. "
            f"Targets: {', '.join(sorted(targets))}."
        )

    if warnings:
        if not quiet:
            print("Preflight warnings:")
            for warning in warnings:
                print(f"  - {warning}")
        if strict and not errors:
            if quiet:
                print("Preflight checks failed (warnings treated as errors).")
            else:
                print("Preflight warnings treated as errors (--strict enabled).")
            return 1

    if errors:
        print("Preflight errors:")
        for error in errors:
            if isinstance(error, dict):
                print(f"  - {json.dumps(error, sort_keys=True)}")
            else:
                print(f"  - {error}")
        if quiet:
            print("Preflight checks failed.")
        return 1

    print("Preflight checks passed.")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Preflight validation for dataset collector pipelines."
    )
    ap.add_argument("--repo-root", default=".", help="Repository root containing pipelines")
    ap.add_argument(
        "--pipeline-map",
        default="src/tools/pipeline_map.sample.yaml",
        help="Pipeline map YAML",
    )
    ap.add_argument(
        "--pipelines",
        nargs="*",
        default=None,
        help="Specific pipelines to check (default: all)",
    )
    ap.add_argument("--strict", action="store_true", help="Treat warnings as failures")
    ap.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    warn_group = ap.add_mutually_exclusive_group()
    warn_group.add_argument(
        "--warn-disabled",
        action="store_true",
        help="Emit warnings for disabled targets",
    )
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress warnings and info output (still prints errors and summary)",
    )
    args = ap.parse_args(argv)

    repo_root = Path(args.repo_root).expanduser().resolve()
    pipeline_map_path = Path(args.pipeline_map).expanduser()
    if not pipeline_map_path.is_absolute():
        pipeline_map_path = repo_root / pipeline_map_path
    pipeline_map_path = pipeline_map_path.resolve()

    return run_preflight(
        repo_root=repo_root,
        pipeline_map_path=pipeline_map_path,
        strict=args.strict,
        pipelines=args.pipelines,
        warn_disabled=args.warn_disabled,
        verbose=args.verbose,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    raise SystemExit(main())
