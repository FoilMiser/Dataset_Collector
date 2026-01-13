#!/usr/bin/env python3
"""Unified CLI for running pipeline stages via configuration."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

# Import registry to ensure specs are registered
import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core import catalog_builder, merge, review_queue
from collector_core.acquire.context import RootsDefaults
from collector_core.acquire_strategies import run_acquire_worker
from collector_core.pipeline_factory import run_pipeline
from collector_core.pipeline_registry import resolve_acquire_hooks, resolve_pipeline_context
from collector_core.pipeline_spec import list_pipelines
from collector_core.targets_paths import list_targets_files
from collector_core.yellow_screen_dispatch import get_yellow_screen_main

STAGE_ACQUIRE = "acquire"
STAGE_MERGE = "merge"
STAGE_YELLOW = "yellow_screen"
STAGE_YELLOW_ALIAS = "screen_yellow"
COMMAND_REVIEW_QUEUE = "review-queue"
COMMAND_CATALOG_BUILDER = "catalog-builder"


def _has_arg(args: list[str], name: str) -> bool:
    return any(item == name or item.startswith(f"{name}=") for item in args)


def _run_with_args(func: Callable[[], int | None], argv: list[str]) -> int:
    original_argv = sys.argv
    try:
        sys.argv = [original_argv[0], *argv]
        result = func()
        return 0 if result is None else int(result)
    finally:
        sys.argv = original_argv


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dataset Collector CLI.")
    parser.add_argument(
        "--list-pipelines",
        action="store_true",
        help="List all available pipelines.",
    )
    sub = parser.add_subparsers(dest="command")
    run = sub.add_parser("run", help="Run a pipeline stage.")
    run.add_argument(
        "--pipeline", help="Pipeline id or slug (e.g. physics_pipeline_v2 or physics)."
    )
    run.add_argument(
        "--stage",
        required=True,
        choices=[STAGE_ACQUIRE, STAGE_MERGE, STAGE_YELLOW, STAGE_YELLOW_ALIAS],
        help="Pipeline stage to run (use yellow_screen; screen_yellow is deprecated).",
    )
    run.add_argument(
        "--repo-root",
        default=".",
        help="Repository root containing pipeline directories (default: .).",
    )
    run.add_argument(
        "--dataset-root",
        default=None,
        help="Override dataset root for stage outputs.",
    )
    run.add_argument(
        "--allow-data-root",
        action="store_true",
        help="Allow /data defaults for stage outputs (default: disabled).",
    )
    run.add_argument("args", nargs=argparse.REMAINDER)

    # Add pipeline subcommand for running full pipeline drivers
    pipeline_parser = sub.add_parser("pipeline", help="Run a full pipeline driver.")
    pipeline_parser.add_argument("domain", help="Pipeline domain (e.g., chem, physics).")
    pipeline_parser.add_argument("args", nargs=argparse.REMAINDER)

    review_parser = sub.add_parser(COMMAND_REVIEW_QUEUE, help="Run the YELLOW review queue helper.")
    review_parser.add_argument(
        "--pipeline",
        "--pipeline-id",
        dest="pipeline",
        help="Pipeline id or slug (e.g. physics_pipeline_v2 or physics).",
    )
    review_parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root containing pipeline directories (default: .).",
    )
    review_parser.add_argument("args", nargs=argparse.REMAINDER)

    catalog_parser = sub.add_parser(COMMAND_CATALOG_BUILDER, help="Run the catalog builder.")
    catalog_parser.add_argument(
        "--pipeline",
        "--pipeline-id",
        dest="pipeline",
        help="Pipeline id or slug (e.g. physics_pipeline_v2 or physics).",
    )
    catalog_parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root containing pipeline directories (default: .).",
    )
    catalog_parser.add_argument("args", nargs=argparse.REMAINDER)

    return parser.parse_args()


def _resolve_targets_arg(args: list[str], targets_path: Path | None, flag: str) -> list[str]:
    if not targets_path or _has_arg(args, flag):
        return args
    return [flag, str(targets_path), *args]


def _resolve_dataset_root_arg(args: list[str], dataset_root: str | None) -> list[str]:
    if not dataset_root or _has_arg(args, "--dataset-root"):
        return args
    return ["--dataset-root", dataset_root, *args]


def _resolve_allow_data_root_arg(args: list[str], allow_data_root: bool) -> list[str]:
    if not allow_data_root or _has_arg(args, "--allow-data-root"):
        return args
    return ["--allow-data-root", *args]


def _resolve_acquire_label(targets_path: Path | None) -> str:
    if targets_path:
        return targets_path.name
    return "targets.yaml"


def _resolve_default_targets(
    args: list[str],
    *,
    ctx,
) -> list[str]:
    return _resolve_targets_arg(args, ctx.targets_path, "--targets")


def _run_review_queue(passthrough: list[str], *, ctx) -> int:
    passthrough = _resolve_default_targets(passthrough, ctx=ctx)
    return _run_with_args(lambda: review_queue.main(pipeline_id=ctx.pipeline_id), passthrough)


def _run_catalog_builder(passthrough: list[str], *, ctx, repo_root: Path) -> int:
    if not _has_arg(passthrough, "--targets"):
        if ctx.targets_path:
            passthrough = ["--targets", str(ctx.targets_path), *passthrough]
        else:
            available = [path.name for path in list_targets_files(repo_root)]
            if available:
                options = ", ".join(available)
                raise SystemExit(
                    "Multiple targets YAML files found. "
                    f"Pass --targets. Options: {options}"
                )
            raise SystemExit("Missing --targets and no default targets YAML could be found.")
    return _run_with_args(lambda: catalog_builder.main(pipeline_id=ctx.pipeline_id), passthrough)


def _run_acquire(slug: str, targets_path: Path | None, args: list[str], ctx) -> int:
    args = _resolve_targets_arg(args, targets_path, "--targets-yaml")
    defaults = RootsDefaults(
        raw_root=f"/data/{slug}/raw",
        manifests_root=f"/data/{slug}/_manifests",
        ledger_root=f"/data/{slug}/_ledger",
        logs_root=f"/data/{slug}/_logs",
    )
    handlers, postprocess = resolve_acquire_hooks(ctx)
    return _run_with_args(
        lambda: run_acquire_worker(
            defaults=defaults,
            targets_yaml_label=_resolve_acquire_label(targets_path),
            strategy_handlers=handlers,
            postprocess=postprocess,
        ),
        args,
    )


def _run_merge(pipeline_id: str, slug: str, targets_path: Path | None, args: list[str]) -> int:
    args = _resolve_targets_arg(args, targets_path, "--targets")
    defaults = merge.default_merge_roots(slug)
    return _run_with_args(lambda: merge.main(pipeline_id=pipeline_id, defaults=defaults), args)


def _run_yellow_screen(slug: str, targets_path: Path | None, args: list[str], ctx) -> int:
    args = _resolve_targets_arg(args, targets_path, "--targets")
    # Get yellow_screen override from config (source of truth: configs/pipelines.yaml)
    yellow_screen: str | None = None
    if isinstance(ctx.overrides, dict):
        yellow_screen = ctx.overrides.get("yellow_screen")
    main_fn = get_yellow_screen_main(slug, yellow_screen=yellow_screen)
    return _run_with_args(main_fn, args)


def main() -> int:
    args = _parse_args()

    # Handle --list-pipelines flag
    if args.list_pipelines:
        print("Available pipelines:")
        for domain in list_pipelines():
            print(f"  - {domain}")
        return 0

    if not args.command:
        print("No command specified. Use 'run' or 'pipeline', or --list-pipelines.")
        return 1

    # Handle pipeline command (run full pipeline driver)
    if args.command == "pipeline":
        passthrough = list(args.args)
        if passthrough[:1] == ["--"]:
            passthrough = passthrough[1:]
        return _run_with_args(lambda: run_pipeline(args.domain), passthrough)

    if args.command == "run":
        passthrough = list(args.args)
        if passthrough[:1] == ["--"]:
            passthrough = passthrough[1:]
        if args.stage == STAGE_YELLOW_ALIAS:
            print(
                "Warning: 'screen_yellow' is deprecated; use 'yellow_screen' instead.",
                file=sys.stderr,
            )
            args.stage = STAGE_YELLOW
        passthrough = _resolve_dataset_root_arg(passthrough, args.dataset_root)
        passthrough = _resolve_allow_data_root_arg(passthrough, args.allow_data_root)

        repo_root = Path(args.repo_root).expanduser().resolve()
        ctx = resolve_pipeline_context(pipeline_id=args.pipeline, repo_root=repo_root)
        if args.stage == STAGE_ACQUIRE:
            return _run_acquire(ctx.slug, ctx.targets_path, passthrough, ctx)
        if args.stage == STAGE_MERGE:
            return _run_merge(ctx.pipeline_id, ctx.slug, ctx.targets_path, passthrough)
        if args.stage == STAGE_YELLOW:
            return _run_yellow_screen(ctx.slug, ctx.targets_path, passthrough, ctx)
        return 0

    passthrough = list(args.args)
    if passthrough[:1] == ["--"]:
        passthrough = passthrough[1:]

    repo_root = Path(args.repo_root).expanduser().resolve()
    ctx = resolve_pipeline_context(pipeline_id=args.pipeline, repo_root=repo_root)

    if args.command == COMMAND_REVIEW_QUEUE:
        return _run_review_queue(passthrough, ctx=ctx)

    if args.command == COMMAND_CATALOG_BUILDER:
        return _run_catalog_builder(passthrough, ctx=ctx, repo_root=repo_root)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
