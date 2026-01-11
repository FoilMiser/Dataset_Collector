"""
collector_core/pipeline_spec.py

Defines the specification for a domain pipeline, enabling configuration-driven
pipeline creation instead of duplicated boilerplate files.
"""

from __future__ import annotations
from pathlib import Path

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PipelineSpec:
    """Specification for a domain-specific pipeline."""

    # Required fields
    domain: str  # e.g., "chem", "bio", "physics"
    name: str  # Human-readable name, e.g., "Chemistry Pipeline"

    # Targets configuration
    targets_yaml: str  # e.g., "targets_chem.yaml"

    # Path configuration (derived from domain if not specified)
    domain_prefix: str | None = None  # Defaults to domain

    # Routing configuration
    routing_keys: list[str] = field(default_factory=list)
    routing_confidence_keys: list[str] = field(default_factory=list)
    default_routing: dict[str, Any] = field(
        default_factory=lambda: {
            "subject": "misc",
            "domain": "misc",
            "category": "misc",
            "level": 5,
            "granularity": "target",
        }
    )

    # Custom worker modules (relative to pipeline directory)
    yellow_screen_module: str | None = None  # e.g., "yellow_screen_chem"
    custom_workers: dict[str, str] = field(default_factory=dict)

    # Feature flags
    include_routing_dict_in_row: bool = False

    @property
    def prefix(self) -> str:
        """Return the domain prefix for paths."""
        return self.domain_prefix or self.domain

    @property
    def pipeline_id(self) -> str:
        """Return the pipeline directory name."""
        return f"{self.domain}_pipeline_v2"

    def get_default_roots(self, base_path: str = "/data") -> dict[str, str]:
        """Return default root paths for this pipeline."""
        prefix = self.prefix
        return {
            "raw_root": f"{base_path}/{prefix}/raw",
            "screened_yellow_root": f"{base_path}/{prefix}/screened_yellow",
            "combined_root": f"{base_path}/{prefix}/combined",
            "manifests_root": f"{base_path}/{prefix}/_manifests",
            "queues_root": f"{base_path}/{prefix}/_queues",
            "catalogs_root": f"{base_path}/{prefix}/_catalogs",
            "ledger_root": f"{base_path}/{prefix}/_ledger",
            "pitches_root": f"{base_path}/{prefix}/_pitches",
            "logs_root": f"{base_path}/{prefix}/_logs",
        }


# Registry of all pipeline specifications
PIPELINE_SPECS: dict[str, PipelineSpec] = {}


def register_pipeline(spec: PipelineSpec) -> PipelineSpec:
    """Register a pipeline specification."""
    PIPELINE_SPECS[spec.domain] = spec
    return spec


def get_pipeline_spec(domain: str) -> PipelineSpec | None:
    """Get a pipeline specification by domain."""
    return PIPELINE_SPECS.get(domain)


def list_pipelines() -> list[str]:
    """List all registered pipeline domains."""
    return sorted(PIPELINE_SPECS.keys())
