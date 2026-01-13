"""Shared discovery helpers for pipeline CLI and registry logic.

Issue 2.1 (v3.0): The CLI no longer requires physical *_pipeline_v2/ directories.
Pipelines can now be run using only the registered PipelineSpec and canonical
targets YAML path (pipelines/targets/targets_<domain>.yaml).
"""

from __future__ import annotations

from pathlib import Path

from collector_core.targets_paths import resolve_targets_path


def normalize_pipeline_id(pipeline_id: str | None) -> str | None:
    """Normalize a pipeline ID to the canonical format.

    Accepts:
    - Short domain name (e.g., "physics") -> "physics_pipeline_v2"
    - Full pipeline ID (e.g., "physics_pipeline_v2") -> unchanged
    """
    if not pipeline_id:
        return None
    if pipeline_id.endswith("_pipeline_v2"):
        return pipeline_id
    return f"{pipeline_id}_pipeline_v2"


def resolve_repo_root(path: Path) -> Path:
    """Resolve the repository root from a given path."""
    if path.name.endswith("_pipeline_v2"):
        return path.parent
    return path


def discover_pipeline_dir(repo_root: Path, pipeline_id: str | None) -> Path | None:
    """Discover pipeline directory. Returns None if directory doesn't exist.

    Note: As of v3.0, physical pipeline directories are optional. The CLI can
    work with just the registered PipelineSpec if targets YAML exists in the
    canonical location (pipelines/targets/targets_<domain>.yaml).
    """
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
    """List available pipelines from both directories and registered specs.

    Returns pipeline IDs from:
    1. Physical *_pipeline_v2/ directories
    2. Registered PipelineSpecs (even without physical directories)
    """
    # Get pipelines from physical directories
    dir_pipelines = {path.name for path in repo_root.glob("*_pipeline_v2") if path.is_dir()}

    # Get pipelines from registered specs
    from collector_core.pipeline_spec import list_pipelines
    spec_pipelines = {f"{domain}_pipeline_v2" for domain in list_pipelines()}

    return sorted(dir_pipelines | spec_pipelines)


def available_pipeline_domains(repo_root: Path) -> list[str]:
    """List available pipeline domains (short names without _pipeline_v2 suffix).

    Combines domains from physical directories and registered specs.
    """
    pipeline_ids = available_pipelines(repo_root)
    return sorted(pid.removesuffix("_pipeline_v2") for pid in pipeline_ids)


def pipeline_slug(pipeline_id: str | None, pipeline_dir: Path | None) -> str | None:
    """Extract the domain slug from a pipeline ID or directory."""
    if pipeline_id:
        normalized = normalize_pipeline_id(pipeline_id)
        return normalized.removesuffix("_pipeline_v2") if normalized else None
    if pipeline_dir:
        return pipeline_dir.name.removesuffix("_pipeline_v2")
    return None


def pick_default_targets(repo_root: Path, slug: str | None) -> Path | None:
    """Pick the default targets YAML path for a pipeline slug."""
    if not slug:
        return None
    return resolve_targets_path(repo_root, slug)


def is_registered_pipeline(domain: str) -> bool:
    """Check if a domain is registered as a PipelineSpec."""
    from collector_core.pipeline_spec import get_pipeline_spec
    return get_pipeline_spec(domain) is not None
