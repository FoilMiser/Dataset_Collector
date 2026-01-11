from __future__ import annotations

import argparse
import ast
import json
import shutil
import sys
import traceback
from collections.abc import Iterable
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from collector_core.config_validator import read_yaml
from tools.strategy_registry import (
    get_external_tools,
    get_strategy_requirement_errors,
    get_strategy_spec,
)
from collector_core.targets_paths import resolve_targets_path, targets_root


def _normalize_download(download: dict[str, Any]) -> dict[str, Any]:
    """Normalize download config by merging nested config into parent dict.

    This mirrors the normalization done at runtime in acquire_strategies.normalize_download.
    """
    d = dict(download or {})
    cfg = d.get("config")

    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in d.items() if k != "config"})
        d = merged

    # Zenodo record_id normalization
    if d.get("strategy") == "zenodo":
        if not d.get("record_id") and d.get("record"):
            d["record_id"] = d["record"]
        if not d.get("record_id") and isinstance(d.get("record_ids"), list) and d["record_ids"]:
            d["record_id"] = d["record_ids"][0]

    return d


TOOL_INSTALL_HINTS = {
    "git": "Install Git for Windows: https://git-scm.com/download/win",
    "aria2c": "Install aria2: https://aria2.github.io/",
    "aws": "Install AWS CLI v2: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html",
}


def _load_yaml(path: Path, schema_name: str) -> dict[str, Any]:
    return read_yaml(path, schema_name=schema_name) or {}


def _load_default_strategy_handlers(repo_root: Path) -> set[str]:
    acquire_strategies_path = repo_root / "collector_core" / "acquire_strategies.py"
    if not acquire_strategies_path.exists():
        raise RuntimeError(f"Missing acquire_strategies module: {acquire_strategies_path}")
    source = acquire_strategies_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(acquire_strategies_path))
    handlers_node: ast.AST | None = None

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "DEFAULT_STRATEGY_HANDLERS":
                    handlers_node = node.value
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Name) and target.id == "DEFAULT_STRATEGY_HANDLERS":
                handlers_node = node.value

    if handlers_node is None:
        raise RuntimeError("DEFAULT_STRATEGY_HANDLERS not found in acquire_strategies.py")

    if not isinstance(handlers_node, ast.Dict):
        raise RuntimeError("DEFAULT_STRATEGY_HANDLERS must be a dict literal")

    handler_keys: set[str] = set()
    for key_node in handlers_node.keys:
        if key_node is None:
            raise RuntimeError("DEFAULT_STRATEGY_HANDLERS must not use dict expansions")
        try:
            key_value = ast.literal_eval(key_node)
        except (ValueError, SyntaxError) as exc:
            raise RuntimeError("Non-literal DEFAULT_STRATEGY_HANDLERS key") from exc
        if not isinstance(key_value, str):
            raise RuntimeError("DEFAULT_STRATEGY_HANDLERS keys must be strings")
        handler_keys.add(key_value)

    if not handler_keys:
        raise RuntimeError("DEFAULT_STRATEGY_HANDLERS has no keys")

    return handler_keys


def _extract_strategy_keys(
    handlers_node: ast.AST,
    acquire_worker_path: Path,
    default_handler_keys: set[str],
) -> set[str]:
    if isinstance(handlers_node, ast.Name) and handlers_node.id == "DEFAULT_STRATEGY_HANDLERS":
        return set(default_handler_keys)
    if isinstance(handlers_node, ast.Dict):
        handler_keys: set[str] = set()
        for key_node, value_node in zip(handlers_node.keys, handlers_node.values, strict=True):
            if key_node is None:
                if (
                    isinstance(value_node, ast.Name)
                    and value_node.id == "DEFAULT_STRATEGY_HANDLERS"
                ):
                    handler_keys.update(default_handler_keys)
                    continue
                raise RuntimeError(
                    f"STRATEGY_HANDLERS uses unsupported dict expansion in {acquire_worker_path}"
                )
            try:
                key_value = ast.literal_eval(key_node)
            except (ValueError, SyntaxError) as exc:
                raise RuntimeError(
                    f"Non-literal STRATEGY_HANDLERS key in {acquire_worker_path}"
                ) from exc
            if not isinstance(key_value, str):
                raise RuntimeError(
                    f"STRATEGY_HANDLERS keys must be strings in {acquire_worker_path}"
                )
            handler_keys.add(key_value)
        return handler_keys
    if isinstance(handlers_node, ast.BinOp) and isinstance(handlers_node.op, ast.BitOr):
        left = _extract_strategy_keys(handlers_node.left, acquire_worker_path, default_handler_keys)
        right = _extract_strategy_keys(
            handlers_node.right, acquire_worker_path, default_handler_keys
        )
        return left | right
    raise RuntimeError(f"STRATEGY_HANDLERS must be a dict literal in {acquire_worker_path}")


def _is_thin_wrapper(source: str) -> bool:
    """Check if the source is a thin wrapper that delegates to generic_workers.main_acquire."""
    # Detect wrapper patterns:
    # - from collector_core.generic_workers import main_acquire
    # - from collector_core import generic_workers
    patterns = [
        "from collector_core.generic_workers import main_acquire",
        "from collector_core import generic_workers",
        "generic_workers.main_acquire",
        "main_acquire(",
    ]
    return any(pattern in source for pattern in patterns)


def _load_strategy_handlers(acquire_worker_path: Path, default_handler_keys: set[str]) -> set[str]:
    source = acquire_worker_path.read_text(encoding="utf-8")

    # Check if this is a thin wrapper that delegates to generic_workers
    # These wrappers use DEFAULT_STRATEGY_HANDLERS from collector_core
    if _is_thin_wrapper(source):
        return set(default_handler_keys)

    tree = ast.parse(source, filename=str(acquire_worker_path))
    handlers_node: ast.AST | None = None

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "STRATEGY_HANDLERS":
                    handlers_node = node.value
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Name) and target.id == "STRATEGY_HANDLERS":
                handlers_node = node.value

    if handlers_node is None:
        # If no STRATEGY_HANDLERS found, treat as thin wrapper using defaults
        return set(default_handler_keys)

    handler_keys = _extract_strategy_keys(handlers_node, acquire_worker_path, default_handler_keys)

    if not handler_keys:
        raise RuntimeError(f"STRATEGY_HANDLERS has no keys in {acquire_worker_path}")

    return handler_keys


def run_preflight(
    repo_root: Path,
    pipeline_map_path: Path,
    strict: bool = False,
    pipelines: list[str] | None = None,
    warn_disabled: bool = False,
    verbose: bool = False,
    quiet: bool = False,
) -> int:
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
    default_handler_keys = _load_default_strategy_handlers(repo_root)

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

        acquire_worker_path = pipeline_dir / "acquire_worker.py"
        if not acquire_worker_path.exists():
            errors.append(f"Acquire worker missing: {acquire_worker_path}")
            continue

        try:
            handler_keys = _load_strategy_handlers(acquire_worker_path, default_handler_keys)
        except Exception as exc:
            errors.append(
                {
                    "pipeline": pipeline_name,
                    "path": str(acquire_worker_path),
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
            f"'{strategy}'. Add it to tools/strategy_registry.py. "
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
        default="tools/pipeline_map.sample.yaml",
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
