"""Pipeline configuration and plugin registry for the unified CLI."""
from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

from collector_core import acquire_strategies
from collector_core.config_validator import read_yaml

STRATEGY_HANDLER = Callable[[acquire_strategies.AcquireContext, dict[str, Any], Path], list[dict[str, Any]]]
POSTPROCESSOR = Callable[
    [acquire_strategies.AcquireContext, dict[str, Any], Path, str, dict[str, Any]],
    dict[str, Any] | None,
]


@dataclass(frozen=True)
class PipelineContext:
    pipeline_id: str
    slug: str
    pipeline_dir: Path
    targets_path: Path | None
    overrides: dict[str, Any]


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
    repo_root = _resolve_repo_root(repo_root)
    pipeline_dir = _discover_pipeline_dir(repo_root, pipeline_id)
    if pipeline_id:
        normalized = _normalize_pipeline_id(pipeline_id)
        if not pipeline_dir or not normalized:
            available = ", ".join(_available_pipelines(repo_root)) or "none"
            raise SystemExit(f"Unknown pipeline '{pipeline_id}'. Available: {available}")
        slug = normalized.removesuffix("_pipeline_v2")
    else:
        if not pipeline_dir:
            raise SystemExit("Missing --pipeline and no pipeline directory detected from the current path.")
        normalized = pipeline_dir.name
        slug = normalized.removesuffix("_pipeline_v2")

    overrides = _load_overrides(repo_root).get(slug, {})
    targets_path = _pick_default_targets(pipeline_dir)
    return PipelineContext(
        pipeline_id=normalized,
        slug=slug,
        pipeline_dir=pipeline_dir,
        targets_path=targets_path,
        overrides=overrides if isinstance(overrides, dict) else {},
    )


def _load_plugin_module(pipeline_dir: Path, module_path: str) -> ModuleType:
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
    overrides = ctx.overrides.get("acquire", {}) if isinstance(ctx.overrides, dict) else {}
    plugin_module = overrides.get("plugin_module")
    if plugin_module:
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
                raise SystemExit(f"Plugin module {plugin_module} missing postprocess {postprocess_name}")
        return handlers, postprocess

    http_handler = overrides.get("http_handler", "multi")
    if http_handler == "single":
        http = acquire_strategies.handle_http_single
    else:
        http = acquire_strategies.handle_http_multi

    handlers: dict[str, STRATEGY_HANDLER] = {
        "http": http,
        "ftp": acquire_strategies.handle_ftp,
        "git": acquire_strategies.handle_git,
        "zenodo": acquire_strategies.handle_zenodo,
        "dataverse": acquire_strategies.handle_dataverse,
        "huggingface_datasets": acquire_strategies.handle_hf_datasets,
    }

    figshare_variant = overrides.get("figshare_variant")
    if figshare_variant == "article":
        handlers["figshare"] = acquire_strategies.handle_figshare_article
    elif figshare_variant == "files":
        handlers["figshare"] = acquire_strategies.handle_figshare_files

    github_release_repo = overrides.get("github_release_repo")
    if github_release_repo:
        handlers["github_release"] = acquire_strategies.make_github_release_handler(
            str(github_release_repo)
        )

    extra_handlers = overrides.get("extra_handlers") or []
    if isinstance(extra_handlers, list):
        extra_map = {
            "s3_sync": acquire_strategies.handle_s3_sync,
            "aws_requester_pays": acquire_strategies.handle_aws_requester_pays,
            "torrent": acquire_strategies.handle_torrent,
        }
        for name in extra_handlers:
            handler = extra_map.get(str(name))
            if handler:
                handlers[str(name)] = handler

    return handlers, None
