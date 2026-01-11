"""Shared discovery helpers for pipeline CLI and registry logic."""

from __future__ import annotations

from pathlib import Path

from collector_core.targets_paths import resolve_targets_path


def normalize_pipeline_id(pipeline_id: str | None) -> str | None:
    if not pipeline_id:
        return None
    if pipeline_id.endswith("_pipeline_v2"):
        return pipeline_id
    return f"{pipeline_id}_pipeline_v2"


def resolve_repo_root(path: Path) -> Path:
    if path.name.endswith("_pipeline_v2") and (path / "pipeline_driver.py").exists():
        return path.parent
    return path


def discover_pipeline_dir(repo_root: Path, pipeline_id: str | None) -> Path | None:
    if pipeline_id:
        normalized = normalize_pipeline_id(pipeline_id)
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


def available_pipelines(repo_root: Path) -> list[str]:
    return sorted(path.name for path in repo_root.glob("*_pipeline_v2") if path.is_dir())


def pipeline_slug(pipeline_id: str | None, pipeline_dir: Path | None) -> str | None:
    if pipeline_id:
        normalized = normalize_pipeline_id(pipeline_id)
        return normalized.removesuffix("_pipeline_v2") if normalized else None
    if pipeline_dir:
        return pipeline_dir.name.removesuffix("_pipeline_v2")
    return None


def pick_default_targets(repo_root: Path, slug: str | None) -> Path | None:
    if not slug:
        return None
    return resolve_targets_path(repo_root, slug)
