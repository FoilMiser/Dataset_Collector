#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import importlib
import shutil
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import yaml

STRATEGY_DEPENDENCIES: Dict[str, List[str]] = {
    "huggingface_datasets": ["datasets", "pyarrow"],
    "s3_public": ["boto3"],
}

STRATEGY_EXTERNAL_TOOLS: Dict[str, List[str]] = {
    "git": ["git"],
    "s3_sync": ["aws"],
    "aws_requester_pays": ["aws"],
}


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_yaml(path: Path) -> Dict[str, object]:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - failure path
        raise ValueError(f"Failed to parse YAML {path}: {exc}") from exc
    if not isinstance(content, dict):
        raise ValueError(f"Expected mapping at top-level in {path}")
    return content


def extract_strategy_handlers(worker_path: Path) -> Set[str]:
    if not worker_path.exists():
        return set()
    try:
        tree = ast.parse(worker_path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "STRATEGY_HANDLERS":
                    if isinstance(node.value, ast.Dict):
                        keys = []
                        for key in node.value.keys:
                            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                                keys.append(key.value)
                        return set(keys)
    return set()


def normalize_strategy(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def record_dependency_warnings(strategies: Iterable[str], warnings: List[str]) -> None:
    needed_deps: Set[str] = set()
    for strat in strategies:
        needed_deps.update(STRATEGY_DEPENDENCIES.get(strat, []))
    for dep in sorted(needed_deps):
        try:
            importlib.import_module(dep)
        except Exception as exc:  # pragma: no cover - optional deps
            warnings.append(f"Optional dependency '{dep}' is not importable: {exc}")


def record_tool_warnings(strategies: Iterable[str], warnings: List[str]) -> None:
    needed_tools: Set[str] = set()
    for strat in strategies:
        needed_tools.update(STRATEGY_EXTERNAL_TOOLS.get(strat, []))
    for tool in sorted(needed_tools):
        if shutil.which(tool) is None:
            warnings.append(f"External tool '{tool}' is required by enabled targets but was not found on PATH")


def validate_targets(
    pipeline_name: str,
    pipeline_dir: Path,
    targets_path: Path,
    strategies_needed: Set[str],
    errors: List[str],
    warnings: List[str],
) -> None:
    try:
        cfg = load_yaml(targets_path)
    except ValueError as exc:
        errors.append(str(exc))
        return

    targets = cfg.get("targets")
    if targets is None:
        errors.append(f"{pipeline_name}: missing 'targets' list in {targets_path}")
        return
    if not isinstance(targets, list):
        errors.append(f"{pipeline_name}: 'targets' must be a list in {targets_path}")
        return

    strategy_handlers = extract_strategy_handlers(pipeline_dir / "acquire_worker.py")
    if not strategy_handlers:
        warnings.append(f"{pipeline_name}: unable to determine supported strategies (missing or unparsable acquire_worker.py)")

    for target in targets:
        if not isinstance(target, dict):
            errors.append(f"{pipeline_name}: target entry must be a mapping in {targets_path}")
            continue
        if not target.get("enabled", True):
            continue
        target_id = target.get("id", "<missing id>")
        download = target.get("download") or {}
        if not isinstance(download, dict):
            errors.append(f"{pipeline_name}:{target_id}: download section must be a mapping")
            continue
        strategy = normalize_strategy(download.get("strategy"))
        if not strategy or strategy == "none":
            errors.append(f"{pipeline_name}:{target_id}: enabled target missing supported download.strategy")
            continue
        strategies_needed.add(strategy)
        if strategy_handlers and strategy not in strategy_handlers:
            errors.append(
                f"{pipeline_name}:{target_id}: download.strategy '{strategy}' not supported by {pipeline_dir / 'acquire_worker.py'}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate pipeline map + targets configuration.")
    parser.add_argument(
        "--pipeline-map",
        default="tools/pipeline_map.yaml",
        help="Path to pipeline_map.yaml (default: tools/pipeline_map.yaml)",
    )
    args = parser.parse_args()

    repo_root = resolve_repo_root()
    map_path = Path(args.pipeline_map)
    if not map_path.is_absolute():
        map_path = repo_root / map_path

    errors: List[str] = []
    warnings: List[str] = []
    strategies_needed: Set[str] = set()

    if not map_path.exists():
        errors.append(f"Pipeline map not found: {map_path}")
    else:
        try:
            map_cfg = load_yaml(map_path)
        except ValueError as exc:
            errors.append(str(exc))
            map_cfg = {}

        pipelines = map_cfg.get("pipelines") if isinstance(map_cfg, dict) else None
        if not isinstance(pipelines, dict):
            errors.append(f"Missing or invalid 'pipelines' mapping in {map_path}")
            pipelines = {}

        for pipeline_name, entry in pipelines.items():
            if not isinstance(entry, dict):
                errors.append(f"Pipeline entry for '{pipeline_name}' must be a mapping")
                continue
            dest_folder = entry.get("dest_folder")
            targets_yaml = entry.get("targets_yaml")
            if not dest_folder:
                errors.append(f"{pipeline_name}: missing dest_folder in {map_path}")
            if not targets_yaml:
                errors.append(f"{pipeline_name}: missing targets_yaml in {map_path}")
                continue
            pipeline_dir = repo_root / pipeline_name
            if not pipeline_dir.exists():
                errors.append(f"{pipeline_name}: pipeline directory not found at {pipeline_dir}")
                continue
            targets_path = Path(targets_yaml)
            if not targets_path.is_absolute():
                targets_path = pipeline_dir / targets_path
            if not targets_path.exists():
                errors.append(f"{pipeline_name}: targets YAML not found at {targets_path}")
                continue
            validate_targets(pipeline_name, pipeline_dir, targets_path, strategies_needed, errors, warnings)

    if strategies_needed:
        record_dependency_warnings(strategies_needed, warnings)
        record_tool_warnings(strategies_needed, warnings)

    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  - {warning}")

    if errors:
        print("Errors:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("Preflight checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
