"""Pipeline configuration and plugin registry for the unified CLI.

Issue 2.1 (v3.0): Physical *_pipeline_v2/ directories are now optional.
Pipelines can be run using only the registered PipelineSpec and canonical
targets YAML path (pipelines/targets/targets_<domain>.yaml).
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from collector_core.acquire.context import PostProcessor, StrategyHandler
from collector_core.acquire.strategies import registry as strategies_registry
from collector_core.config_validator import read_yaml
from collector_core.pipeline_discovery import (
    available_pipelines,
    discover_pipeline_dir,
    is_registered_pipeline,
    normalize_pipeline_id,
    pick_default_targets,
    resolve_repo_root,
)

STRATEGY_HANDLER = StrategyHandler
POSTPROCESSOR = PostProcessor


@dataclass(frozen=True)
class PipelineContext:
    """Context for running a pipeline stage.

    Attributes:
        pipeline_id: The full pipeline ID (e.g., "physics_pipeline_v2")
        slug: The domain slug (e.g., "physics")
        pipeline_dir: The physical pipeline directory (may be None if using spec-only mode)
        targets_path: Path to the targets YAML file
        overrides: Configuration overrides from configs/pipelines.yaml
    """

    pipeline_id: str
    slug: str
    pipeline_dir: Path | None  # Now optional - can be None for spec-only pipelines
    targets_path: Path | None
    overrides: dict[str, Any]


def _load_overrides(repo_root: Path) -> dict[str, Any]:
    config_path = repo_root / "configs" / "pipelines.yaml"
    if not config_path.exists():
        return {}
    data = read_yaml(config_path) or {}
    pipelines = data.get("pipelines", {})
    if isinstance(pipelines, dict):
        return pipelines
    return {}


def resolve_pipeline_context(*, pipeline_id: str | None, repo_root: Path) -> PipelineContext:
    """Resolve pipeline context for the unified CLI.

    Issue 2.1 (v3.0): Now supports running pipelines without physical directories.
    A pipeline can run if it meets ANY of these conditions:
    1. Physical *_pipeline_v2/ directory exists
    2. PipelineSpec is registered AND targets YAML exists in canonical location

    Args:
        pipeline_id: Pipeline ID or domain slug (e.g., "physics" or "physics_pipeline_v2")
        repo_root: Repository root path

    Returns:
        PipelineContext with resolved configuration

    Raises:
        SystemExit: If pipeline cannot be found or no targets YAML exists
    """
    repo_root = resolve_repo_root(repo_root)
    pipeline_dir = discover_pipeline_dir(repo_root, pipeline_id)

    if pipeline_id:
        normalized = normalize_pipeline_id(pipeline_id)
        if not normalized:
            available = ", ".join(available_pipelines(repo_root)) or "none"
            raise SystemExit(f"Unknown pipeline '{pipeline_id}'. Available: {available}")

        slug = normalized.removesuffix("_pipeline_v2")

        # Issue 2.1: Check if pipeline is valid via spec OR directory
        if not pipeline_dir and not is_registered_pipeline(slug):
            available = ", ".join(available_pipelines(repo_root)) or "none"
            raise SystemExit(f"Unknown pipeline '{pipeline_id}'. Available: {available}")
    else:
        if not pipeline_dir:
            raise SystemExit(
                "Missing --pipeline and no pipeline directory detected from the current path."
            )
        normalized = pipeline_dir.name
        slug = normalized.removesuffix("_pipeline_v2")

    overrides = _load_overrides(repo_root).get(slug, {})
    targets_path = pick_default_targets(repo_root, slug)

    # Issue 2.1: Warn if running without physical directory (spec-only mode)
    if not pipeline_dir and targets_path:
        import logging

        logger = logging.getLogger(__name__)
        logger.debug(
            "Running pipeline '%s' in spec-only mode (no physical directory)", slug
        )

    return PipelineContext(
        pipeline_id=normalized,
        slug=slug,
        pipeline_dir=pipeline_dir,
        targets_path=targets_path,
        overrides=overrides if isinstance(overrides, dict) else {},
    )


def _load_plugin_module(pipeline_dir: Path | None, module_path: str) -> ModuleType:
    """Load a plugin module from the pipeline directory.

    Args:
        pipeline_dir: The pipeline directory (can be None for spec-only pipelines)
        module_path: Relative path to the module within the pipeline directory

    Raises:
        SystemExit: If pipeline_dir is None or module not found
    """
    if pipeline_dir is None:
        raise SystemExit(
            f"Plugin module '{module_path}' requires a physical pipeline directory. "
            "This pipeline is running in spec-only mode without a *_pipeline_v2/ directory."
        )
    resolved = (pipeline_dir / module_path).resolve()
    if not resolved.exists():
        raise SystemExit(f"Pipeline plugin module not found: {resolved}")
    module_name = f"dc_plugin_{pipeline_dir.name}_{resolved.stem}"
    spec = importlib.util.spec_from_file_location(module_name, resolved)
    if not spec or not spec.loader:
        raise SystemExit(f"Unable to load plugin module: {resolved}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def resolve_acquire_hooks(
    ctx: PipelineContext,
) -> tuple[dict[str, STRATEGY_HANDLER], POSTPROCESSOR | None]:
    """Resolve acquisition handlers and postprocessors for a pipeline.

    Issue 2.1 (v3.0): Now supports spec-only pipelines (no physical directory).
    Plugin modules still require a physical directory, but default handlers work
    without one.
    """
    overrides = ctx.overrides.get("acquire", {}) if isinstance(ctx.overrides, dict) else {}
    plugin_module = overrides.get("plugin_module")
    if plugin_module:
        # Plugin modules require a physical pipeline directory
        module = _load_plugin_module(ctx.pipeline_dir, str(plugin_module))
        handlers = getattr(module, "STRATEGY_HANDLERS", None)
        if handlers is None:
            raise SystemExit(f"Plugin module {plugin_module} missing STRATEGY_HANDLERS")
        if not isinstance(handlers, dict):
            raise SystemExit(f"Plugin module {plugin_module} STRATEGY_HANDLERS must be a dict")
        wrap_name = overrides.get("wrap_handlers")
        if wrap_name:
            wrapper = getattr(module, str(wrap_name), None)
            if not callable(wrapper):
                raise SystemExit(f"Plugin module {plugin_module} missing wrap handler {wrap_name}")
            handlers = {key: wrapper(handler) for key, handler in handlers.items()}
        postprocess_name = overrides.get("postprocess")
        postprocess = None
        if postprocess_name:
            postprocess = getattr(module, str(postprocess_name), None)
            if postprocess_name and not callable(postprocess):
                raise SystemExit(
                    f"Plugin module {plugin_module} missing postprocess {postprocess_name}"
                )
        return handlers, postprocess

    # Default handlers work without a physical pipeline directory
    figshare_variant = overrides.get("figshare_variant")
    if figshare_variant is not None:
        figshare_variant = str(figshare_variant)

    extra_handlers = overrides.get("extra_handlers") or []
    if not isinstance(extra_handlers, list):
        extra_handlers = []

    handlers = strategies_registry.build_default_handlers(
        http_handler=str(overrides.get("http_handler", "multi")),
        figshare_variant=figshare_variant if figshare_variant in {"article", "files"} else None,
        github_release_repo=str(overrides["github_release_repo"])
        if overrides.get("github_release_repo")
        else None,
        extra_handlers=[str(name) for name in extra_handlers],
    )

    return handlers, None
