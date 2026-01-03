from __future__ import annotations

import argparse
import importlib.util
import shutil
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from tools.strategy_registry import get_external_tools, get_strategy_spec

TOOL_INSTALL_HINTS = {
    "git": "Install Git for Windows: https://git-scm.com/download/win",
    "aria2c": "Install aria2: https://aria2.github.io/",
    "aws": "Install AWS CLI v2: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html",
}


def _load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


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


def _iter_enabled_targets(targets: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for target in targets:
        enabled = target.get("enabled", True)
        if enabled:
            yield target


def run_preflight(repo_root: Path, pipeline_map_path: Path, strict: bool = False) -> int:
    pipeline_map = _load_yaml(pipeline_map_path)
    pipelines_cfg = pipeline_map.get("pipelines", {}) or {}
    errors: list[str] = []
    warnings: list[str] = []
    strategies_in_use: set[str] = set()
    strategy_targets: dict[str, list[str]] = {}
    registry_misses: dict[str, list[str]] = {}

    for pipeline_name, pipeline_entry in pipelines_cfg.items():
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

        targets_cfg = _load_yaml(targets_path)
        targets = targets_cfg.get("targets", []) or []
        for target in _iter_enabled_targets(targets):
            target_id = target.get("id", "<unknown>")
            download = target.get("download", {}) or {}
            strategy = (download.get("strategy") or "").strip()
            if not strategy or strategy == "none":
                errors.append(
                    f"{pipeline_name}:{target_id} enabled with missing/none download.strategy"
                )
                continue
            if strategy not in handler_keys:
                errors.append(
                    f"{pipeline_name}:{target_id} uses unsupported strategy '{strategy}'"
                )
                continue
            if get_strategy_spec(strategy) is None:
                registry_misses.setdefault(strategy, []).append(f"{pipeline_name}:{target_id}")
            strategies_in_use.add(strategy)
            strategy_targets.setdefault(strategy, []).append(f"{pipeline_name}:{target_id}")

    for strategy in sorted(strategies_in_use):
        for tool in get_external_tools(strategy):
            if shutil.which(tool) is None:
                targets = ", ".join(sorted(strategy_targets.get(strategy, [])))
                hint = TOOL_INSTALL_HINTS.get(tool)
                warnings.append(
                    f"Missing external tool '{tool}' required by strategy '{strategy}'."
                    + (f" Targets: {targets}." if targets else "")
                    + (f" {hint}" if hint else "")
                )

    for strategy, targets in sorted(registry_misses.items()):
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
    ap.add_argument("--pipeline-map", default="tools/pipeline_map.yaml", help="Pipeline map YAML")
    ap.add_argument("--strict", action="store_true", help="Treat warnings as failures")
    args = ap.parse_args(argv)

    repo_root = Path(args.repo_root).expanduser().resolve()
    pipeline_map_path = Path(args.pipeline_map).expanduser()
    if not pipeline_map_path.is_absolute():
        pipeline_map_path = repo_root / pipeline_map_path
    pipeline_map_path = pipeline_map_path.resolve()

    return run_preflight(repo_root=repo_root, pipeline_map_path=pipeline_map_path, strict=args.strict)


if __name__ == "__main__":
    raise SystemExit(main())
