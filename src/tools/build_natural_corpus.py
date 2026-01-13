from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path, PurePath, PureWindowsPath

from collector_core.config_validator import read_yaml
from collector_core.targets_paths import resolve_targets_path, targets_root
from tools.init_layout import init_layout
from tools.patch_targets import patch_targets_yaml
from tools.preflight import run_preflight

DEFAULT_STAGES = [
    "classify",
    "acquire_green",
    "acquire_yellow",
    "yellow_screen",
    "merge",
    "catalog",
]
PIPELINE_MAP_PLACEHOLDER = "YOUR_DATASET_ROOT_HERE"
MODE_STAGES = {
    "collect": ["classify", "acquire_green", "acquire_yellow"],
    "compile": ["yellow_screen", "merge", "catalog"],
    "full": DEFAULT_STAGES,
}


def _is_windows_style(path_str: str) -> bool:
    return ":" in path_str or "\\" in path_str


def _normalize_list_arg(values: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        if "," in value:
            normalized.extend([part for part in value.split(",") if part])
        else:
            normalized.append(value)
    return normalized


def _load_pipeline_map(path: Path) -> dict[str, object]:
    return read_yaml(path, schema_name="pipeline_map") or {}


def _validate_pipeline_map_destination(dest_root: str | None, pipeline_map_path: Path) -> None:
    if not dest_root or dest_root == PIPELINE_MAP_PLACEHOLDER:
        raise SystemExit(
            "Pipeline map destination_root is not configured. "
            "Set destination_root in src/tools/pipeline_map.local.yaml or pass --dest-root."
            f" (Current map: {pipeline_map_path})"
        )


def _dataset_root_paths(dest_root: str, dest_folder: str) -> tuple[Path, PurePath]:
    fs_root = Path(dest_root) / dest_folder
    if _is_windows_style(dest_root):
        yaml_root = PureWindowsPath(dest_root) / dest_folder
    else:
        yaml_root = Path(dest_root) / dest_folder
    return fs_root, yaml_root


def pick_existing(queues_root: Path, candidates: list[str]) -> Path:
    for name in candidates:
        path = queues_root / name
        if path.exists():
            return path
    raise FileNotFoundError(
        f"None of {candidates} exist under {queues_root}. Did you run the classify stage first?"
    )


def _run_stage(
    repo_root: Path,
    pipeline_slug: str,
    stage: str,
    targets_path: Path,
    queues_root: Path,
    catalogs_root: Path,
    workers: int,
    execute: bool,
    log_path: Path,
) -> None:
    python_exe = sys.executable
    cli_module = [python_exe, "-m", "collector_core.dc_cli"]
    cmd: list[str]
    if stage == "classify":
        cmd = [
            *cli_module,
            "pipeline",
            pipeline_slug,
            "--targets",
            str(targets_path),
        ]
        if not execute:
            cmd.append("--no-fetch")
    elif stage == "acquire_green":
        queue_path = pick_existing(queues_root, ["green_download.jsonl", "green_queue.jsonl"])
        cmd = [
            *cli_module,
            "run",
            "--pipeline",
            pipeline_slug,
            "--stage",
            "acquire",
            "--",
            "--queue",
            str(queue_path),
            "--targets-yaml",
            str(targets_path),
            "--bucket",
            "green",
            "--workers",
            str(workers),
        ]
        if execute:
            cmd.append("--execute")
    elif stage == "acquire_yellow":
        queue_path = pick_existing(queues_root, ["yellow_pipeline.jsonl", "yellow_queue.jsonl"])
        cmd = [
            *cli_module,
            "run",
            "--pipeline",
            pipeline_slug,
            "--stage",
            "acquire",
            "--",
            "--queue",
            str(queue_path),
            "--targets-yaml",
            str(targets_path),
            "--bucket",
            "yellow",
            "--workers",
            str(workers),
        ]
        if execute:
            cmd.append("--execute")
    elif stage == "yellow_screen":
        queue_path = pick_existing(queues_root, ["yellow_pipeline.jsonl", "yellow_queue.jsonl"])
        cmd = [
            *cli_module,
            "run",
            "--pipeline",
            pipeline_slug,
            "--stage",
            "yellow_screen",
            "--",
            "--targets",
            str(targets_path),
            "--queue",
            str(queue_path),
        ]
        if execute:
            cmd.append("--execute")
    elif stage == "merge":
        cmd = [
            *cli_module,
            "run",
            "--pipeline",
            pipeline_slug,
            "--stage",
            "merge",
            "--",
            "--targets",
            str(targets_path),
        ]
        if execute:
            cmd.append("--execute")
    elif stage == "catalog":
        cmd = [
            *cli_module,
            "catalog-builder",
            "--pipeline",
            pipeline_slug,
            "--targets",
            str(targets_path),
            "--output",
            str(catalogs_root / "catalog.json"),
        ]
    else:
        raise ValueError(f"Unknown stage: {stage}")

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_file:
        subprocess.run(
            cmd,
            cwd=repo_root,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=True,
        )


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build the Natural corpus across all pipelines.")
    ap.add_argument("--repo-root", default=".", help="Repository root containing pipelines")
    ap.add_argument(
        "--pipeline-map",
        default="src/tools/pipeline_map.sample.yaml",
        help="Pipeline map YAML",
    )
    ap.add_argument("--dest-root", default=None, help="Destination root for Natural corpus")
    ap.add_argument("--pipelines", nargs="+", default=["all"], help="Pipelines to run or 'all'")
    ap.add_argument("--stages", nargs="+", default=DEFAULT_STAGES, help="Stages to execute")
    ap.add_argument(
        "--mode", choices=sorted(MODE_STAGES.keys()), default="full", help="Stage preset to run"
    )
    ap.add_argument("--workers", type=int, default=8, help="Worker count for acquisition")
    ap.add_argument("--execute", action="store_true", help="Execute pipeline stages")
    args = ap.parse_args(argv)

    repo_root = Path(args.repo_root).expanduser().resolve()
    pipeline_map_path = Path(args.pipeline_map).expanduser()
    if not pipeline_map_path.is_absolute():
        pipeline_map_path = repo_root / pipeline_map_path
    pipeline_map_path = pipeline_map_path.resolve()
    if run_preflight(repo_root=repo_root, pipeline_map_path=pipeline_map_path) != 0:
        sys.exit(1)
    pipeline_map = _load_pipeline_map(pipeline_map_path)
    dest_root = args.dest_root or pipeline_map.get("destination_root")
    _validate_pipeline_map_destination(dest_root, pipeline_map_path)

    pipelines_cfg = pipeline_map.get("pipelines", {}) or {}
    selected_pipelines = _normalize_list_arg(args.pipelines)
    if "all" in selected_pipelines:
        pipeline_names = list(pipelines_cfg.keys())
    else:
        pipeline_names = selected_pipelines

    if args.mode != "full":
        stages = MODE_STAGES[args.mode]
    else:
        stages = _normalize_list_arg(args.stages)

    for pipeline_name in pipeline_names:
        pipeline_entry = pipelines_cfg.get(pipeline_name)
        if not pipeline_entry:
            raise SystemExit(f"Unknown pipeline: {pipeline_name}")

        dest_folder = pipeline_entry.get("dest_folder")
        targets_yaml = pipeline_entry.get("targets_yaml")
        if not dest_folder or not targets_yaml:
            raise SystemExit(f"Pipeline map entry incomplete for {pipeline_name}.")

        slug = pipeline_name.removesuffix("_pipeline_v2")
        targets_path = resolve_targets_path(repo_root, slug, targets_yaml)
        if not targets_path or not targets_path.exists():
            expected = targets_root(repo_root) / targets_yaml
            raise SystemExit(f"Targets YAML not found: {expected}")

        dataset_root_fs, dataset_root_yaml = _dataset_root_paths(dest_root, dest_folder)
        init_layout(dataset_root_fs)

        patched_targets_dir = dataset_root_fs / "_manifests" / "_patched_targets"
        patched_targets_path = patched_targets_dir / f"{targets_path.stem}_patched.yaml"
        patch_targets_yaml(targets_path, dataset_root_yaml, patched_targets_path)

        queues_root = dataset_root_fs / "_queues"
        catalogs_root = dataset_root_fs / "_catalogs"

        for stage in stages:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_path = (
                dataset_root_fs / "_logs" / f"orchestrator_{pipeline_name}_{stage}_{timestamp}.log"
            )
            _run_stage(
                repo_root=repo_root,
                pipeline_slug=slug,
                stage=stage,
                targets_path=patched_targets_path,
                queues_root=queues_root,
                catalogs_root=catalogs_root,
                workers=args.workers,
                execute=args.execute,
                log_path=log_path,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
