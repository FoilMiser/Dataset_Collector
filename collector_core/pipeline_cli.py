#!/usr/bin/env python3
"""Unified CLI for running pipeline helpers from a single entrypoint."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

from collector_core import catalog_builder, review_queue

COMMAND_REVIEW_QUEUE = "review-queue"
COMMAND_CATALOG_BUILDER = "catalog-builder"


def _normalize_pipeline_id(pipeline_id: str | None) -> str | None:
    if not pipeline_id:
        return None
    if pipeline_id.endswith("_pipeline_v2"):
        return pipeline_id
    return f"{pipeline_id}_pipeline_v2"


def _resolve_repo_root(path: Path) -> Path:
    if path.name.endswith("_pipeline_v2") and (path / "pipeline_driver.py").exists():
        return path.parent
    return path


def _discover_pipeline_dir(repo_root: Path, pipeline_id: str | None) -> Path | None:
    if pipeline_id:
        normalized = _normalize_pipeline_id(pipeline_id)
        if normalized:
            candidate = repo_root / normalized
            if candidate.is_dir():
                return candidate
            slug = normalized.removesuffix("_pipeline_v2")
            for path in sorted(repo_root.glob("*_pipeline_v2")):
                if path.name == f"{slug}_pipeline_v2":
                    return path
        return None
    cwd = Path.cwd().resolve()
    for path in (cwd, *cwd.parents):
        if path.name.endswith("_pipeline_v2"):
            return path
    return None


def _available_pipelines(repo_root: Path) -> list[str]:
    return sorted(path.name for path in repo_root.glob("*_pipeline_v2") if path.is_dir())


def _pick_default_targets(pipeline_dir: Path | None) -> Path | None:
    if not pipeline_dir:
        return None
    targets = sorted(pipeline_dir.glob("targets_*.yaml"))
    if len(targets) == 1:
        return targets[0]
    return None


def _has_targets_arg(args: list[str]) -> bool:
    for item in args:
        if item == "--targets" or item.startswith("--targets="):
            return True
    return False


def _run_module_main(
    main_func: Callable[..., int | None],
    argv: list[str],
    *,
    pipeline_id: str | None,
) -> int:
    original_argv = sys.argv
    try:
        sys.argv = [original_argv[0], *argv]
        result = main_func(pipeline_id=pipeline_id)
        return 0 if result is None else int(result)
    finally:
        sys.argv = original_argv


def _run_review_queue(argv: list[str], pipeline_id: str | None) -> int:
    return _run_module_main(review_queue.main, argv, pipeline_id=pipeline_id)


def _run_catalog_builder(argv: list[str], pipeline_id: str | None) -> int:
    return _run_module_main(catalog_builder.main, argv, pipeline_id=pipeline_id)


def _resolve_pipeline_context(
    *,
    pipeline_id: str | None,
    repo_root: Path,
) -> tuple[str | None, Path | None]:
    pipeline_dir = _discover_pipeline_dir(repo_root, pipeline_id)
    if pipeline_id:
        normalized = _normalize_pipeline_id(pipeline_id)
        if not pipeline_dir:
            available = ", ".join(_available_pipelines(repo_root)) or "none"
            raise SystemExit(f"Unknown pipeline '{pipeline_id}'. Available: {available}")
        return normalized, pipeline_dir
    if pipeline_dir:
        return pipeline_dir.name, pipeline_dir
    return None, None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unified CLI for Dataset Collector pipeline helpers.",
    )
    parser.add_argument(
        "--pipeline-id",
        help="Pipeline id or slug (e.g. physics_pipeline_v2 or physics).",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root containing pipeline directories (default: .).",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    review_parser = sub.add_parser(COMMAND_REVIEW_QUEUE, help="Run the YELLOW review queue helper.")
    review_parser.add_argument("args", nargs=argparse.REMAINDER)
    catalog_parser = sub.add_parser(COMMAND_CATALOG_BUILDER, help="Run the catalog builder.")
    catalog_parser.add_argument("args", nargs=argparse.REMAINDER)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    repo_root = _resolve_repo_root(Path(args.repo_root).expanduser().resolve())
    pipeline_id, pipeline_dir = _resolve_pipeline_context(
        pipeline_id=args.pipeline_id,
        repo_root=repo_root,
    )

    passthrough = list(args.args)
    default_targets = _pick_default_targets(pipeline_dir)

    if args.command == COMMAND_CATALOG_BUILDER:
        if not _has_targets_arg(passthrough):
            if default_targets:
                passthrough = ["--targets", str(default_targets), *passthrough]
            else:
                available = []
                if pipeline_dir:
                    available = sorted(p.name for p in pipeline_dir.glob("targets_*.yaml"))
                if available:
                    options = ", ".join(available)
                    raise SystemExit(
                        f"Multiple targets YAML files found in {pipeline_dir}. "
                        f"Pass --targets. Options: {options}"
                    )
                raise SystemExit("Missing --targets and no default targets YAML could be found.")
        return _run_catalog_builder(passthrough, pipeline_id)

    if args.command == COMMAND_REVIEW_QUEUE:
        if default_targets and not _has_targets_arg(passthrough):
            passthrough = ["--targets", str(default_targets), *passthrough]
        return _run_review_queue(passthrough, pipeline_id)

    return 0


def run_deprecated_entrypoint(command: str, *, pipeline_id: str | None) -> int:
    message = (
        f"Deprecated entrypoint: {command}. Use `dc-pipeline {command} --pipeline-id {pipeline_id}` "
        "or `python -m collector_core.pipeline_cli`."
    )
    print(message, file=sys.stderr)
    passthrough = sys.argv[1:]
    if command == COMMAND_REVIEW_QUEUE:
        return _run_review_queue(passthrough, pipeline_id)
    if command == COMMAND_CATALOG_BUILDER:
        return _run_catalog_builder(passthrough, pipeline_id)
    raise SystemExit(f"Unknown deprecated command: {command}")


if __name__ == "__main__":
    raise SystemExit(main())
