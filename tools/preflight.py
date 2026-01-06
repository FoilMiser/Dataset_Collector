from __future__ import annotations

import argparse
import importlib.util
import shutil
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from collector_core.config_validator import read_yaml
from tools.strategy_registry import get_external_tools, get_strategy_spec

TOOL_INSTALL_HINTS = {
    "git": "Install Git for Windows: https://git-scm.com/download/win",
    "aria2c": "Install aria2: https://aria2.github.io/",
    "aws": "Install AWS CLI v2: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html",
}


def _load_yaml(path: Path, schema_name: str) -> dict[str, Any]:
    return read_yaml(path, schema_name=schema_name) or {}


def _load_strategy_handlers(acquire_worker_path: Path) -> set[str]:
    module_name = f"acquire_worker_{acquire_worker_path.parent.name}"
    spec = importlib.util.spec_from_file_location(module_name, acquire_worker_path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Unable to load module from {acquire_worker_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    handlers = getattr(module, "STRATEGY_HANDLERS", None)
    if not isinstance(handlers, dict):
        raise RuntimeError(f"STRATEGY_HANDLERS not found in {acquire_worker_path}")
    return set(handlers.keys())


def run_preflight(
    repo_root: Path,
    pipeline_map_path: Path,
    strict: bool = False,
    pipelines: list[str] | None = None,
    warn_disabled: bool = False,
) -> int:
    pipeline_map = _load_yaml(pipeline_map_path, "pipeline_map")
    pipelines_cfg = pipeline_map.get("pipelines", {}) or {}
    errors: list[str] = []
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

    for pipeline_name, pipeline_entry in pipeline_items:
        pipeline_dir = repo_root / pipeline_name
        if not pipeline_dir.exists():
            errors.append(f"Pipeline directory missing: {pipeline_dir}")
            continue

        targets_yaml = pipeline_entry.get("targets_yaml")
        if not targets_yaml:
            errors.append(f"Pipeline map entry missing targets_yaml for: {pipeline_name}")
            continue

        targets_path = pipeline_dir / targets_yaml
        if not targets_path.exists():
            errors.append(f"Targets YAML missing: {targets_path}")
            continue

        acquire_worker_path = pipeline_dir / "acquire_worker.py"
        if not acquire_worker_path.exists():
            errors.append(f"Acquire worker missing: {acquire_worker_path}")
            continue

        try:
            handler_keys = _load_strategy_handlers(acquire_worker_path)
        except RuntimeError as exc:
            errors.append(str(exc))
            continue

        targets_cfg = _load_yaml(targets_path, "targets")
        targets = targets_cfg.get("targets", []) or []
        for target in targets:
            enabled = target.get("enabled", True)
            target_id = target.get("id", "<unknown>")
            download = target.get("download", {}) or {}
            strategy = (download.get("strategy") or "").strip()
            if not strategy or strategy == "none":
                if enabled:
                    errors.append(
                        f"{pipeline_name}:{target_id} enabled with missing/none download.strategy"
                    )
                elif warn_disabled:
                    warnings.append(
                        f"{pipeline_name}:{target_id} disabled with missing/none download.strategy"
                    )
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
            if get_strategy_spec(strategy) is None:
                if enabled:
                    registry_misses_enabled.setdefault(strategy, []).append(
                        f"{pipeline_name}:{target_id}"
                    )
                else:
                    registry_misses_disabled.setdefault(strategy, []).append(
                        f"{pipeline_name}:{target_id}"
                    )
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
                    f"Missing external tool '{tool}' required by strategy '{strategy}'."
                    + (f" Targets: {targets}." if targets else "")
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
                        f"Missing external tool '{tool}' required by strategy '{strategy}'."
                        + (f" Targets: {targets}." if targets else "")
                        + (f" {hint}" if hint else "")
                    )

    registry_strategies = set(registry_misses_enabled.keys()) | set(
        registry_misses_disabled.keys()
    )
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
        print("Preflight warnings:")
        for warning in warnings:
            print(f"  - {warning}")
        if strict and not errors:
            print("Preflight warnings treated as errors (--strict enabled).")
            return 1

    if errors:
        print("Preflight errors:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("Preflight checks passed.")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Preflight validation for dataset collector pipelines.")
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
    warn_group = ap.add_mutually_exclusive_group()
    warn_group.add_argument(
        "--warn-disabled",
        action="store_true",
        help="Emit warnings for disabled targets",
    )
    warn_group.add_argument(
        "--quiet",
        action="store_true",
        help="Deprecated alias for suppressing warnings for disabled targets",
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
    )


if __name__ == "__main__":
    raise SystemExit(main())
