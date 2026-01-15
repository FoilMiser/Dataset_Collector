"""
collector_core/pipeline_factory.py

Factory for creating pipeline driver instances from specifications.
Eliminates the need for per-pipeline pipeline_driver.py files.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import BasePipelineDriver, RoutingBlockSpec
from collector_core.pipeline_spec import PipelineSpec, get_pipeline_spec, list_pipelines

if TYPE_CHECKING:
    pass


def create_pipeline_driver(spec: PipelineSpec) -> type[BasePipelineDriver]:
    """
    Create a pipeline driver class from a specification.

    This dynamically generates a subclass of BasePipelineDriver configured
    according to the PipelineSpec, eliminating the need for boilerplate
    pipeline_driver.py files in each pipeline directory.
    """

    # Build routing blocks from routing keys
    routing_blocks = []
    for key in spec.routing_keys:
        routing_blocks.append(RoutingBlockSpec(name=key, sources=[key], mode="subset"))

    # Create the dynamic class
    class_attrs = {
        "DOMAIN": spec.domain,
        "PIPELINE_VERSION": VERSION,
        "TARGETS_LABEL": spec.targets_yaml,
        "USER_AGENT": f"{spec.domain}-corpus-pipeline",
        "ROUTING_KEYS": spec.routing_keys,
        "ROUTING_CONFIDENCE_KEYS": spec.routing_confidence_keys,
        "DEFAULT_ROUTING": spec.default_routing,
        "ROUTING_BLOCKS": routing_blocks,
        "INCLUDE_ROUTING_DICT_IN_ROW": spec.include_routing_dict_in_row,
    }

    driver_class = type(
        f"{spec.domain.title().replace('_', '')}PipelineDriver",
        (BasePipelineDriver,),
        class_attrs,
    )

    return driver_class


def get_pipeline_driver(domain: str) -> type[BasePipelineDriver]:
    """
    Get a pipeline driver class for a domain.

    Args:
        domain: The domain identifier (e.g., "chem", "physics")

    Returns:
        A configured pipeline driver class

    Raises:
        ValueError: If the domain is not registered
    """
    spec = get_pipeline_spec(domain)
    if spec is None:
        available = ", ".join(list_pipelines())
        raise ValueError(f"Unknown pipeline domain: {domain}. Available: {available}")
    return create_pipeline_driver(spec)


def run_pipeline(domain: str) -> None:
    """
    Run a pipeline by domain name.

    This is the main entry point for running a pipeline from the command line.
    """
    driver_class = get_pipeline_driver(domain)
    driver_class.main()
