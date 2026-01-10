#!/usr/bin/env python3
"""Unified CLI for running pipeline stages via configuration."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

from collector_core import merge, yellow_screen_standard
from collector_core.acquire_strategies import RootsDefaults, run_acquire_worker
from collector_core.pipeline_registry import resolve_acquire_hooks, resolve_pipeline_context
from collector_core.yellow_screen_common import default_yellow_roots
from collector_core.yellow_screen_chem import main as yellow_screen_chem
from collector_core.yellow_screen_econ import main as yellow_screen_econ
from collector_core.yellow_screen_kg_nav import main as yellow_screen_kg_nav
from collector_core.yellow_screen_nlp import main as yellow_screen_nlp
from collector_core.yellow_screen_safety import main as yellow_screen_safety

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
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Run a pipeline stage.")
    run.add_argument("--pipeline", help="Pipeline id or slug (e.g. physics_pipeline_v2 or physics).")
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
    module = (ctx.overrides.get("yellow_screen") or "standard") if isinstance(ctx.overrides, dict) else "standard"
    if module == "chem":
        return _run_with_args(yellow_screen_chem, args)
    if module == "econ":
        return _run_with_args(yellow_screen_econ, args)
    if module == "kg_nav":
        return _run_with_args(yellow_screen_kg_nav, args)
    if module == "nlp":
        return _run_with_args(yellow_screen_nlp, args)
    if module == "safety":
        return _run_with_args(yellow_screen_safety, args)

    defaults = default_yellow_roots(slug)
    return _run_with_args(lambda: yellow_screen_standard.main(defaults=defaults), args)


def main() -> int:
    args = _parse_args()
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
