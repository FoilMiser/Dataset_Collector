#!/usr/bin/env python3
"""Unified CLI for running pipeline stages via configuration."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

# Import registry to ensure specs are registered
import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core import merge
from collector_core.acquire_strategies import RootsDefaults, run_acquire_worker
from collector_core.pipeline_factory import run_pipeline
from collector_core.pipeline_registry import resolve_acquire_hooks, resolve_pipeline_context
from collector_core.pipeline_spec import list_pipelines
from collector_core.yellow_screen_dispatch import get_yellow_screen_main

STAGE_ACQUIRE = "acquire"
STAGE_MERGE = "merge"
STAGE_YELLOW = "yellow_screen"


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
        choices=[STAGE_ACQUIRE, STAGE_MERGE, STAGE_YELLOW],
        help="Pipeline stage to run.",
    )
    run.add_argument(
        "--repo-root",
        default=".",
        help="Repository root containing pipeline directories (default: .).",
    )
    run.add_argument("args", nargs=argparse.REMAINDER)

    # Add pipeline subcommand for running full pipeline drivers
    pipeline_parser = sub.add_parser("pipeline", help="Run a full pipeline driver.")
    pipeline_parser.add_argument("domain", help="Pipeline domain (e.g., chem, physics).")
    pipeline_parser.add_argument("args", nargs=argparse.REMAINDER)

    return parser.parse_args()


def _resolve_targets_arg(args: list[str], targets_path: Path | None, flag: str) -> list[str]:
    if not targets_path or _has_arg(args, flag):
        return args
    return [flag, str(targets_path), *args]


def _resolve_acquire_label(targets_path: Path | None) -> str:
    if targets_path:
        return targets_path.name
    return "targets.yaml"


def _run_acquire(slug: str, targets_path: Path | None, args: list[str], ctx) -> int:
    args = _resolve_targets_arg(args, targets_path, "--targets-yaml")
    defaults = RootsDefaults(
        raw_root=f"/data/{slug}/raw",
        manifests_root=f"/data/{slug}/_manifests",
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
    try:
        main_fn = get_yellow_screen_main(slug)
        return _run_with_args(main_fn, args)
    except ValueError:
        # Fallback: if domain is not registered, check overrides
        module = (
            (ctx.overrides.get("yellow_screen") or "standard")
            if isinstance(ctx.overrides, dict)
            else "standard"
        )
        if module != "standard":
            # Try to get from dispatch with override module name
            from collector_core.pipeline_spec import get_pipeline_spec

            spec = get_pipeline_spec(slug)
            if spec is None:
                # Create a temporary lookup using the override module
                import importlib

                module_name = f"collector_core.yellow_screen_{module}"
                try:
                    mod = importlib.import_module(module_name)
                    return _run_with_args(mod.main, args)
                except ImportError:
                    pass
        # Ultimate fallback to standard
        from collector_core import yellow_screen_standard
        from collector_core.yellow_screen_common import default_yellow_roots

        defaults = default_yellow_roots(slug)
        return _run_with_args(lambda: yellow_screen_standard.main(defaults=defaults), args)


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

    # Handle run command (individual stages)
    passthrough = list(args.args)
    if passthrough[:1] == ["--"]:
        passthrough = passthrough[1:]

    repo_root = Path(args.repo_root).expanduser().resolve()
    ctx = resolve_pipeline_context(pipeline_id=args.pipeline, repo_root=repo_root)

    if args.command == "run":
        if args.stage == STAGE_ACQUIRE:
            return _run_acquire(ctx.slug, ctx.targets_path, passthrough, ctx)
        if args.stage == STAGE_MERGE:
            return _run_merge(ctx.pipeline_id, ctx.slug, ctx.targets_path, passthrough)
        if args.stage == STAGE_YELLOW:
            return _run_yellow_screen(ctx.slug, ctx.targets_path, passthrough, ctx)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
