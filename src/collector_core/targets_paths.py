"""Helpers for locating targets YAML files within the repository layout."""

from __future__ import annotations

from pathlib import Path

from collector_core import pipeline_specs_registry  # noqa: F401
from collector_core.pipeline_spec import get_pipeline_spec


def targets_root(repo_root: Path) -> Path:
    """Return the root directory for targets YAML files."""
    return repo_root / "pipelines" / "targets"


def resolve_targets_path(
    repo_root: Path,
    slug: str,
    targets_yaml: str | None = None,
) -> Path | None:
    """Resolve the targets YAML path for a pipeline slug."""
    root = targets_root(repo_root)
    candidates: list[str] = []
    if targets_yaml:
        candidates.append(targets_yaml)
    spec = get_pipeline_spec(slug)
    if spec and spec.targets_yaml not in candidates:
        candidates.append(spec.targets_yaml)
    default = f"targets_{slug}.yaml"
    if default not in candidates:
        candidates.append(default)

    for name in candidates:
        path = root / name
        if path.exists():
            return path
    return None


def list_targets_files(repo_root: Path) -> list[Path]:
    """List all targets YAML files in the repo."""
    root = targets_root(repo_root)
    if not root.exists():
        return []
    return sorted(root.glob("targets_*.yaml"))
