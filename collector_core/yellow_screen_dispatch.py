"""
collector_core/yellow_screen_dispatch.py

Unified dispatcher for yellow screen workers. Routes to domain-specific
modules when configured in PipelineSpec, otherwise uses yellow_screen_standard.
"""
from __future__ import annotations

import importlib
from typing import Any

from collector_core import yellow_screen_standard
from collector_core.pipeline_spec import get_pipeline_spec
from collector_core.yellow_screen_common import default_yellow_roots


def get_yellow_screen_main(domain: str):
    """
    Return the main() function for the given domain's yellow screen worker.

    If the domain's PipelineSpec has a yellow_screen_module configured,
    imports and returns its main(). Otherwise, returns a wrapper around
    yellow_screen_standard.main() with domain-specific defaults.
    """
    spec = get_pipeline_spec(domain)
    if spec is None:
        raise ValueError(f"Unknown domain: {domain}")

    if spec.yellow_screen_module:
        module_name = f"collector_core.{spec.yellow_screen_module}"
        try:
            mod = importlib.import_module(module_name)
            return mod.main
        except ImportError as e:
            raise ImportError(
                f"Failed to import yellow screen module {module_name} for domain {domain}: {e}"
            ) from e

    defaults = default_yellow_roots(spec.prefix)

    def _standard_main() -> None:
        yellow_screen_standard.main(defaults=defaults)

    return _standard_main


def main_yellow_screen(domain: str) -> None:
    """
    Entry point for running yellow screen for a domain.
    Dispatches to the appropriate module based on PipelineSpec.
    """
    main_fn = get_yellow_screen_main(domain)
    main_fn()


__all__ = ["get_yellow_screen_main", "main_yellow_screen"]
