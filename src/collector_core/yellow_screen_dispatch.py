"""
collector_core/yellow_screen_dispatch.py

Unified dispatcher for yellow screen workers. Routes to domain-specific
modules when configured via yellow_screen parameter, otherwise uses standard.
"""

from __future__ import annotations

from collections.abc import Callable

from collector_core.__version__ import __version__ as VERSION  # noqa: F401
from collector_core.pipeline_spec import get_pipeline_spec
from collector_core.stability import stable_api
from collector_core.yellow_screen_common import default_yellow_roots


@stable_api
def get_yellow_screen_main(
    domain: str, *, yellow_screen: str | None = None
) -> Callable[[], None]:
    """
    Return the main() function for the given domain's yellow screen worker.

    Args:
        domain: Pipeline domain slug (e.g., "chem", "physics")
        yellow_screen: Optional override for yellow screen domain module.
            If provided and not "standard", uses collector_core.yellow.domains.<yellow_screen>.
            If None, falls back to standard yellow screen.

    Returns:
        A callable main() function for running the yellow screen stage.

    Raises:
        ValueError: If domain is not registered or yellow_screen module not found.
    """
    spec = get_pipeline_spec(domain)
    if spec is None:
        raise ValueError(f"Unknown domain: {domain}")

    defaults = default_yellow_roots(spec.prefix)

    # Determine yellow screen module: explicit override > standard
    effective_yellow = yellow_screen

    if effective_yellow and effective_yellow != "standard":
        # Import from collector_core.yellow.domains
        from collector_core.yellow import domains as yellow_domains

        domain_mod = getattr(yellow_domains, effective_yellow, None)
        if domain_mod is None:
            available = [
                name for name in dir(yellow_domains) if not name.startswith("_")
            ]
            raise ValueError(
                f"Yellow screen module '{effective_yellow}' not found in "
                f"collector_core.yellow.domains. Available: {', '.join(available)}"
            )

        def _domain_main() -> None:
            # Lazy import to avoid requiring datasets module at module load time
            from collector_core.yellow.base import run_yellow_screen

            run_yellow_screen(defaults=defaults, domain=domain_mod)

        return _domain_main

    def _standard_main() -> None:
        # Lazy import to avoid requiring datasets module at module load time
        from collector_core.yellow.base import run_yellow_screen
        from collector_core.yellow.domains import standard

        run_yellow_screen(defaults=defaults, domain=standard)

    return _standard_main


@stable_api
def main_yellow_screen(domain: str, *, yellow_screen: str | None = None) -> None:
    """
    Entry point for running yellow screen for a domain.
    Dispatches to the appropriate module based on configuration.

    Args:
        domain: Pipeline domain slug (e.g., "chem", "physics")
        yellow_screen: Optional override for yellow screen domain module.
    """
    main_fn = get_yellow_screen_main(domain, yellow_screen=yellow_screen)
    main_fn()


__all__ = ["get_yellow_screen_main", "main_yellow_screen"]
