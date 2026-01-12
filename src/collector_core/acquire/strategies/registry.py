"""Registry for acquisition strategy handlers with lazy loading.

This module provides lazy-loading registration of strategy handlers to minimize
import-time overhead. Strategy modules are only imported when actually used.

Usage:
    from collector_core.acquire.strategies.registry import (
        get_handler,
        build_default_handlers,
    )

    # Get a specific handler (lazy loads the module)
    http_handler = get_handler("http")

    # Build all default handlers
    handlers = build_default_handlers()
"""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collector_core.acquire.context import StrategyHandler

logger = logging.getLogger(__name__)

# Module cache for lazy loading
_module_cache: dict[str, Any] = {}


def _lazy_import(module_name: str) -> Any:
    """Lazily import a strategy module.

    Args:
        module_name: Name of the module (e.g., "http", "git")

    Returns:
        The imported module

    Raises:
        ImportError: If the module cannot be imported
    """
    if module_name not in _module_cache:
        full_name = f"collector_core.acquire.strategies.{module_name}"
        _module_cache[module_name] = importlib.import_module(full_name)
    return _module_cache[module_name]


# Strategy registry mapping names to lazy loader functions
_STRATEGY_LOADERS: dict[str, tuple[str, str, dict[str, Any]]] = {
    # (module_name, loader_func, kwargs)
    "http": ("http", "resolve_http_handler", {"variant": "multi"}),
    "http_single": ("http", "resolve_http_handler", {"variant": "single"}),
    "http_multi": ("http", "resolve_http_handler", {"variant": "multi"}),
    "ftp": ("ftp", "get_handler", {}),
    "git": ("git", "get_handler", {}),
    "zenodo": ("zenodo", "get_handler", {}),
    "dataverse": ("dataverse", "get_handler", {}),
    "figshare": ("figshare", "get_handler", {}),
    "figshare_article": ("figshare", "resolve_figshare_handler", {"variant": "article"}),
    "figshare_files": ("figshare", "resolve_figshare_handler", {"variant": "files"}),
    "huggingface_datasets": ("hf", "get_handler", {}),
    "hf": ("hf", "get_handler", {}),
    "s3_sync": ("s3", "get_sync_handler", {}),
    "aws_requester_pays": ("s3", "get_requester_pays_handler", {}),
    "torrent": ("torrent", "get_handler", {}),
}


def get_handler(name: str, **kwargs: Any) -> "StrategyHandler":
    """Get a strategy handler by name with lazy loading.

    This function lazily imports the strategy module only when needed,
    reducing startup time for applications that don't use all strategies.

    Args:
        name: Strategy name (e.g., "http", "git", "zenodo")
        **kwargs: Additional arguments passed to the handler factory

    Returns:
        The strategy handler function

    Raises:
        ValueError: If the strategy name is not recognized
        ImportError: If the strategy module fails to import
    """
    if name not in _STRATEGY_LOADERS:
        available = ", ".join(sorted(_STRATEGY_LOADERS.keys()))
        raise ValueError(f"Unknown strategy '{name}'. Available: {available}")

    module_name, func_name, default_kwargs = _STRATEGY_LOADERS[name]
    merged_kwargs = {**default_kwargs, **kwargs}

    module = _lazy_import(module_name)
    loader = getattr(module, func_name)

    if merged_kwargs:
        return loader(**merged_kwargs)
    return loader()


def list_strategies() -> list[str]:
    """List all available strategy names.

    Returns:
        Sorted list of strategy names
    """
    return sorted(_STRATEGY_LOADERS.keys())


def is_strategy_available(name: str) -> bool:
    """Check if a strategy is available.

    Args:
        name: Strategy name

    Returns:
        True if the strategy exists
    """
    return name in _STRATEGY_LOADERS


def register_strategy(
    name: str,
    module_name: str,
    loader_func: str,
    **default_kwargs: Any,
) -> None:
    """Register a custom strategy handler.

    Args:
        name: Strategy name for lookups
        module_name: Module name under acquire.strategies
        loader_func: Function name in module that returns handler
        **default_kwargs: Default arguments for the loader
    """
    _STRATEGY_LOADERS[name] = (module_name, loader_func, dict(default_kwargs))


def build_default_handlers(
    *,
    http_handler: str = "multi",
    figshare_variant: str | None = None,
    github_release_repo: str | None = None,
    extra_handlers: list[str] | None = None,
) -> dict[str, "StrategyHandler"]:
    """Build a dictionary of default strategy handlers.

    This function builds the standard set of handlers for pipeline use.
    All modules are lazy-loaded on first use.

    Args:
        http_handler: HTTP handler variant ("single" or "multi")
        figshare_variant: Figshare handler variant ("article" or "files")
        github_release_repo: GitHub repository for release handler
        extra_handlers: Additional handlers to include

    Returns:
        Dictionary mapping strategy names to handler functions
    """
    handlers: dict[str, "StrategyHandler"] = {
        "http": get_handler("http", variant=http_handler),
        "ftp": get_handler("ftp"),
        "git": get_handler("git"),
        "zenodo": get_handler("zenodo"),
        "dataverse": get_handler("dataverse"),
        "huggingface_datasets": get_handler("huggingface_datasets"),
    }

    if figshare_variant:
        handlers["figshare"] = get_handler(f"figshare_{figshare_variant}")
    else:
        handlers["figshare"] = get_handler("figshare")

    if github_release_repo:
        # GitHub release requires dynamic handler creation
        gh_module = _lazy_import("github_release")
        handlers["github_release"] = gh_module.resolve_handler(github_release_repo)

    if extra_handlers:
        for name in extra_handlers:
            if is_strategy_available(name):
                handlers[name] = get_handler(name)
            else:
                logger.warning(f"Unknown extra handler requested: {name}")

    return handlers


def clear_module_cache() -> None:
    """Clear the module cache (useful for testing)."""
    _module_cache.clear()


# Re-export commonly used items for convenience
__all__ = [
    "get_handler",
    "list_strategies",
    "is_strategy_available",
    "register_strategy",
    "build_default_handlers",
    "clear_module_cache",
]
