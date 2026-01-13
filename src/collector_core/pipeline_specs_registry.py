"""
collector_core/pipeline_specs_registry.py

Registry of all domain pipeline specifications.
This module loads pipeline specs from configs/pipelines.yaml (the authoritative source of truth).

NOTE: This module is now a thin wrapper around pipeline_specs_loader.py.
All pipeline configuration should be done in configs/pipelines.yaml.
"""

from __future__ import annotations

# Re-export for backward compatibility
from collector_core.pipeline_spec import (
    PipelineSpec,
    PIPELINE_SPECS,
    get_pipeline_spec,
    list_pipelines,
    register_pipeline,
)
from collector_core.pipeline_specs_loader import (
    register_pipelines_from_yaml,
    load_pipeline_specs_from_yaml,
    get_yaml_config,
)

# Load and register all pipelines from YAML on module import
register_pipelines_from_yaml()
