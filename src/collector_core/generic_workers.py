"""
collector_core/generic_workers.py

Generic worker implementations that can be parameterized by pipeline spec.
Replaces per-pipeline acquire_worker.py, merge_worker.py, etc.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core import catalog_builder, merge, review_queue
from collector_core.acquire.context import RootsDefaults
from collector_core.acquire_strategies import run_acquire_worker
from collector_core.pipeline_registry import resolve_acquire_hooks, resolve_pipeline_context
from collector_core.pipeline_spec import PipelineSpec, get_pipeline_spec

if TYPE_CHECKING:
    pass


def _resolve_spec(domain: str) -> PipelineSpec:
    spec = get_pipeline_spec(domain)
    if spec is None:
        print(f"Unknown pipeline domain: {domain}", file=sys.stderr)
        sys.exit(1)
    return spec


def _resolve_context(spec: PipelineSpec, repo_root: Path | None) -> object:
    resolved_root = repo_root or Path.cwd()
    return resolve_pipeline_context(pipeline_id=spec.pipeline_id, repo_root=resolved_root)


def run_acquire_for_pipeline(spec: PipelineSpec, repo_root: Path | None = None) -> None:
    """Run the acquire worker for a pipeline specification."""
    ctx = _resolve_context(spec, repo_root)
    roots = spec.get_default_roots()
    defaults = RootsDefaults(
        raw_root=roots["raw_root"],
        manifests_root=roots["manifests_root"],
        ledger_root=roots["ledger_root"],
        logs_root=roots["logs_root"],
    )
    handlers, postprocess = resolve_acquire_hooks(ctx)
    run_acquire_worker(
        defaults=defaults,
        targets_yaml_label=spec.targets_yaml,
        strategy_handlers=handlers,
        postprocess=postprocess,
    )


def run_merge_for_pipeline(spec: PipelineSpec) -> None:
    defaults = merge.default_merge_roots(spec.prefix)
    merge.main(pipeline_id=spec.pipeline_id, defaults=defaults)


def run_catalog_for_pipeline(spec: PipelineSpec) -> None:
    catalog_builder.main(pipeline_id=spec.pipeline_id)


def run_review_queue_for_pipeline(spec: PipelineSpec) -> None:
    review_queue.main(pipeline_id=spec.pipeline_id)


def main_acquire(domain: str, *, repo_root: Path | None = None) -> None:
    """Entry point for acquire worker."""
    spec = _resolve_spec(domain)
    run_acquire_for_pipeline(spec, repo_root=repo_root)


def main_merge(domain: str) -> None:
    """Entry point for merge worker."""
    spec = _resolve_spec(domain)
    run_merge_for_pipeline(spec)


def main_catalog(domain: str) -> None:
    """Entry point for catalog builder."""
    spec = _resolve_spec(domain)
    run_catalog_for_pipeline(spec)


def main_review_queue(domain: str) -> None:
    """Entry point for review queue helper."""
    spec = _resolve_spec(domain)
    run_review_queue_for_pipeline(spec)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generic worker entrypoints for pipelines.")
    parser.add_argument("--domain", required=True, help="Pipeline domain slug (e.g. math).")
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root containing pipeline directories (default: .).",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    acquire = sub.add_parser("acquire", help="Run the acquire worker.")
    acquire.add_argument("args", nargs=argparse.REMAINDER)
    merge_cmd = sub.add_parser("merge", help="Run the merge worker.")
    merge_cmd.add_argument("args", nargs=argparse.REMAINDER)
    catalog = sub.add_parser("catalog", help="Run the catalog builder.")
    catalog.add_argument("args", nargs=argparse.REMAINDER)
    review = sub.add_parser("review-queue", help="Run the review queue helper.")
    review.add_argument("args", nargs=argparse.REMAINDER)
    return parser.parse_args()


def _run_with_args(func, argv: list[str]) -> int:
    original_argv = sys.argv
    try:
        sys.argv = [original_argv[0], *argv]
        result = func()
        return 0 if result is None else int(result)
    finally:
        sys.argv = original_argv


def main() -> int:
    args = _parse_args()
    repo_root = Path(args.repo_root).expanduser().resolve()
    passthrough = list(args.args)
    if passthrough[:1] == ["--"]:
        passthrough = passthrough[1:]

    if args.command == "acquire":
        return _run_with_args(
            lambda: main_acquire(args.domain, repo_root=repo_root), passthrough
        )
    if args.command == "merge":
        return _run_with_args(lambda: main_merge(args.domain), passthrough)
    if args.command == "catalog":
        return _run_with_args(lambda: main_catalog(args.domain), passthrough)
    if args.command == "review-queue":
        return _run_with_args(lambda: main_review_queue(args.domain), passthrough)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
