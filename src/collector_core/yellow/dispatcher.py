"""Yellow screening dispatcher module.

This module provides the canonical entry path for yellow screening used by
CLI and workers. It implements lazy loading of domain modules to minimize
import-time overhead.

Usage:
    from collector_core.yellow.dispatcher import (
        dispatch_yellow_screen,
        get_domain,
        list_domains,
    )

    # Run yellow screening for a domain
    dispatch_yellow_screen("chem", targets="targets.yaml", queue="queue.jsonl")

    # Get a specific domain module (lazy loads)
    chem_domain = get_domain("chem")

    # List all available domains
    domains = list_domains()
"""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Any

from collector_core.stability import stable_api

if TYPE_CHECKING:
    from types import ModuleType

logger = logging.getLogger(__name__)

# Module cache for lazy loading
_domain_cache: dict[str, ModuleType] = {}

# Default domain name to fall back to for unknown domains
_DEFAULT_DOMAIN = "standard"

# Domain registry mapping domain names to module names under collector_core.yellow.domains
# Format: domain_name -> module_name
_DOMAIN_REGISTRY: dict[str, str] = {
    "standard": "standard",
    "chem": "chem",
    "econ": "econ",
    "kg_nav": "kg_nav",
    "nlp": "nlp",
    "safety": "safety",
}

# Alias registry for mapping alternative names to canonical domain names
_DOMAIN_ALIASES: dict[str, str] = {
    "chemistry": "chem",
    "chemical": "chem",
    "economics": "econ",
    "economic": "econ",
    "econ_stats_decision_adaptation": "econ",
    "knowledge_graph": "kg_nav",
    "kg": "kg_nav",
    "navigation": "kg_nav",
    "natural_language": "nlp",
    "text": "nlp",
    "safety_incident": "safety",
    "incident": "safety",
}


def _lazy_import_domain(module_name: str) -> ModuleType:
    """Lazily import a domain module.

    Args:
        module_name: Name of the module (e.g., "chem", "standard")

    Returns:
        The imported module

    Raises:
        ImportError: If the module cannot be imported
    """
    if module_name not in _domain_cache:
        full_name = f"collector_core.yellow.domains.{module_name}"
        _domain_cache[module_name] = importlib.import_module(full_name)
    return _domain_cache[module_name]


def _resolve_domain_name(domain_name: str) -> str:
    """Resolve a domain name to its canonical form.

    Handles aliases and falls back to the default domain if unknown.

    Args:
        domain_name: Domain name (may be an alias)

    Returns:
        Canonical domain name
    """
    # Check if it's directly in the registry
    if domain_name in _DOMAIN_REGISTRY:
        return domain_name

    # Check if it's an alias
    if domain_name in _DOMAIN_ALIASES:
        return _DOMAIN_ALIASES[domain_name]

    # Fall back to default domain
    logger.warning(
        "Unknown domain '%s', falling back to '%s' domain",
        domain_name,
        _DEFAULT_DOMAIN,
    )
    return _DEFAULT_DOMAIN


@stable_api
def get_domain(domain_name: str) -> ModuleType:
    """Get a domain module by name with lazy loading.

    This function lazily imports the domain module only when needed,
    reducing startup time for applications that don't use all domains.

    If the domain name is unknown, falls back to the standard domain.

    Args:
        domain_name: Domain name (e.g., "chem", "nlp", "standard")
            Also accepts aliases like "chemistry" for "chem".

    Returns:
        The domain module containing filter_record and transform_record functions

    Raises:
        ImportError: If the domain module fails to import
    """
    canonical_name = _resolve_domain_name(domain_name)
    module_name = _DOMAIN_REGISTRY[canonical_name]
    return _lazy_import_domain(module_name)


@stable_api
def list_domains() -> list[str]:
    """List all available domain names.

    Returns:
        Sorted list of canonical domain names
    """
    return sorted(_DOMAIN_REGISTRY.keys())


@stable_api
def list_domain_aliases() -> dict[str, str]:
    """List all domain aliases and their canonical names.

    Returns:
        Dictionary mapping alias names to canonical domain names
    """
    return dict(_DOMAIN_ALIASES)


@stable_api
def is_domain_available(domain_name: str) -> bool:
    """Check if a domain is available (directly or via alias).

    Args:
        domain_name: Domain name or alias

    Returns:
        True if the domain exists or has an alias
    """
    return domain_name in _DOMAIN_REGISTRY or domain_name in _DOMAIN_ALIASES


@stable_api
def register_domain(
    name: str,
    module_name: str | None = None,
    aliases: list[str] | None = None,
) -> None:
    """Register a custom domain handler.

    Args:
        name: Canonical domain name for lookups
        module_name: Module name under yellow.domains (defaults to name)
        aliases: Optional list of alternative names for the domain
    """
    actual_module = module_name if module_name is not None else name
    _DOMAIN_REGISTRY[name] = actual_module

    if aliases:
        for alias in aliases:
            _DOMAIN_ALIASES[alias] = name


@stable_api
def unregister_domain(name: str) -> bool:
    """Unregister a domain handler.

    Also removes any aliases pointing to this domain.

    Args:
        name: Domain name to remove

    Returns:
        True if the domain was removed, False if it wasn't registered
    """
    if name not in _DOMAIN_REGISTRY:
        return False

    del _DOMAIN_REGISTRY[name]

    # Remove any aliases pointing to this domain
    aliases_to_remove = [
        alias for alias, target in _DOMAIN_ALIASES.items() if target == name
    ]
    for alias in aliases_to_remove:
        del _DOMAIN_ALIASES[alias]

    # Clear from cache if present
    if name in _domain_cache:
        del _domain_cache[name]

    return True


@stable_api
def dispatch_yellow_screen(
    domain_name: str,
    *,
    targets: str | None = None,
    queue: str | None = None,
    execute: bool = False,
    dataset_root: str | None = None,
    allow_data_root: bool = False,
    pitch_sample_limit: int | None = None,
    pitch_text_limit: int | None = None,
    prefix: str | None = None,
    **extra_kwargs: Any,
) -> None:
    """Dispatch yellow screening for a domain.

    This is the canonical entry path for yellow screening, used by CLI and workers.
    Only this dispatch path should be used for running yellow screens.

    Args:
        domain_name: Domain to run screening for (e.g., "chem", "nlp")
        targets: Path to targets.yaml file
        queue: Path to queue JSONL file
        execute: If True, write outputs; if False, dry-run mode
        dataset_root: Override for dataset root directory
        allow_data_root: Allow /data defaults for outputs
        pitch_sample_limit: Max pitch samples per reason (override)
        pitch_text_limit: Max chars stored in pitch samples (override)
        prefix: Domain prefix for default paths (defaults to domain_name)
        **extra_kwargs: Additional arguments passed to run_yellow_screen

    Raises:
        ValueError: If required arguments (targets, queue) are missing
        ImportError: If the domain module fails to import
    """
    import sys

    # Lazy imports to avoid circular dependencies and heavy module loads
    from collector_core.yellow.base import run_yellow_screen
    from collector_core.yellow_screen_common import (
        YellowRootDefaults,
        default_yellow_roots,
    )

    # Get the domain module
    domain = get_domain(domain_name)

    # Determine prefix for default paths
    effective_prefix = prefix if prefix is not None else _resolve_domain_name(domain_name)

    # Build defaults
    defaults: YellowRootDefaults = default_yellow_roots(effective_prefix)

    # Build sys.argv for the argparse-based run_yellow_screen
    # This maintains compatibility with existing CLI infrastructure
    original_argv = sys.argv
    try:
        argv = ["yellow_screen"]

        if targets:
            argv.extend(["--targets", targets])
        if queue:
            argv.extend(["--queue", queue])
        if execute:
            argv.append("--execute")
        if dataset_root:
            argv.extend(["--dataset-root", dataset_root])
        if allow_data_root:
            argv.append("--allow-data-root")
        if pitch_sample_limit is not None:
            argv.extend(["--pitch-sample-limit", str(pitch_sample_limit)])
        if pitch_text_limit is not None:
            argv.extend(["--pitch-text-limit", str(pitch_text_limit)])

        sys.argv = argv
        run_yellow_screen(defaults=defaults, domain=domain)
    finally:
        sys.argv = original_argv


@stable_api
def create_domain_runner(
    domain_name: str,
    prefix: str | None = None,
) -> callable[[], None]:
    """Create a main function for running yellow screening for a specific domain.

    This creates a callable suitable for use as a module's main() function,
    matching the interface expected by existing yellow_screen_* modules.

    Args:
        domain_name: Domain to create runner for
        prefix: Domain prefix for default paths (defaults to domain_name)

    Returns:
        A callable that runs yellow screening when invoked
    """
    from collector_core.yellow.base import run_yellow_screen
    from collector_core.yellow_screen_common import default_yellow_roots

    effective_prefix = prefix if prefix is not None else _resolve_domain_name(domain_name)
    defaults = default_yellow_roots(effective_prefix)

    def _domain_main() -> None:
        domain = get_domain(domain_name)
        run_yellow_screen(defaults=defaults, domain=domain)

    _domain_main.__name__ = f"yellow_screen_{domain_name}_main"
    _domain_main.__doc__ = f"Run yellow screening for the {domain_name} domain."

    return _domain_main


@stable_api
def clear_domain_cache() -> None:
    """Clear the domain module cache (useful for testing)."""
    _domain_cache.clear()


# Re-export commonly used items for convenience
__all__ = [
    "dispatch_yellow_screen",
    "get_domain",
    "list_domains",
    "list_domain_aliases",
    "is_domain_available",
    "register_domain",
    "unregister_domain",
    "create_domain_runner",
    "clear_domain_cache",
]
