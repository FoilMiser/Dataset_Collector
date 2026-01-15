"""
collector_core/pipeline_specs_loader.py

Loads PipelineSpec objects from configs/pipelines.yaml.
This makes pipelines.yaml the authoritative source of truth for pipeline configuration.
"""

from __future__ import annotations

import importlib.resources
from pathlib import Path
from typing import Any

import yaml

from collector_core.pipeline_spec import PipelineSpec, register_pipeline

# Human-readable names for each domain
# This mapping ensures consistent naming across the system
DOMAIN_NAMES: dict[str, str] = {
    "3d_modeling": "3D Modeling Pipeline",
    "agri_circular": "Agriculture & Circular Economy Pipeline",
    "biology": "Biology Pipeline",
    "chem": "Chemistry Pipeline",
    "code": "Code Pipeline",
    "cyber": "Cybersecurity Pipeline",
    "earth": "Earth Science Pipeline",
    "econ_stats_decision_adaptation": "Economics, Statistics, Decision & Adaptation Pipeline",
    "engineering": "Engineering Pipeline",
    "fixture": "Fixture Pipeline",
    "kg_nav": "Knowledge Graph & Navigation Pipeline",
    "logic": "Logic Pipeline",
    "materials_science": "Materials Science Pipeline",
    "math": "Mathematics Pipeline",
    "metrology": "Metrology Pipeline",
    "nlp": "NLP Pipeline",
    "physics": "Physics Pipeline",
    "regcomp": "Regulatory Compliance Pipeline",
    "safety_incident": "Safety Incident Pipeline",
}


def _get_pipelines_yaml_path() -> Path:
    """Get the path to pipelines.yaml, working both installed and in development."""
    # Try importlib.resources first (works when installed)
    try:
        resource_path = importlib.resources.files("configs").joinpath("pipelines.yaml")
        # Use as_posix() to get the path without deprecated context manager
        if hasattr(resource_path, "is_file") and resource_path.is_file():
            return Path(str(resource_path))
    except (TypeError, FileNotFoundError, ModuleNotFoundError, AttributeError):
        pass

    # Fall back to relative path from this file (works in development)
    src_dir = Path(__file__).parent.parent.parent
    yaml_path = src_dir / "configs" / "pipelines.yaml"
    if yaml_path.exists():
        return yaml_path

    # Last resort: check from current working directory
    cwd_path = Path.cwd() / "configs" / "pipelines.yaml"
    if cwd_path.exists():
        return cwd_path

    raise FileNotFoundError("Could not find configs/pipelines.yaml")


def load_pipelines_yaml() -> dict[str, Any]:
    """Load and return the pipelines.yaml contents."""
    yaml_path = _get_pipelines_yaml_path()
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def _extract_targets_filename(targets_path: str) -> str:
    """Extract just the filename from a targets path like 'pipelines/targets/targets_chem.yaml'."""
    return Path(targets_path).name


def _yaml_to_pipeline_spec(domain: str, config: dict[str, Any]) -> PipelineSpec:
    """Convert a single pipeline YAML config to a PipelineSpec object."""
    routing = config.get("routing", {})
    knobs = config.get("knobs", {})

    # Extract targets filename from full path
    targets_yaml = _extract_targets_filename(config.get("targets_path", f"targets_{domain}.yaml"))

    # Get human-readable name
    name = DOMAIN_NAMES.get(domain, f"{domain.replace('_', ' ').title()} Pipeline")

    return PipelineSpec(
        domain=domain,
        name=name,
        targets_yaml=targets_yaml,
        domain_prefix=knobs.get("domain_prefix"),
        routing_keys=routing.get("keys", []),
        routing_confidence_keys=routing.get("confidence_keys", []),
        default_routing=routing.get("default", {
            "subject": "misc",
            "domain": "misc",
            "category": "misc",
            "level": 5,
            "granularity": "target",
        }),
        custom_workers=knobs.get("custom_workers", {}),
        include_routing_dict_in_row=knobs.get("include_routing_dict_in_row", False),
    )


def load_pipeline_specs_from_yaml() -> dict[str, PipelineSpec]:
    """Load all pipeline specs from pipelines.yaml and return them as a dict."""
    data = load_pipelines_yaml()
    pipelines = data.get("pipelines", {})

    specs: dict[str, PipelineSpec] = {}
    for domain, config in pipelines.items():
        spec = _yaml_to_pipeline_spec(domain, config)
        specs[domain] = spec

    return specs


def register_pipelines_from_yaml() -> None:
    """Load pipeline specs from YAML and register them in the global registry."""
    specs = load_pipeline_specs_from_yaml()
    for spec in specs.values():
        register_pipeline(spec)


def get_yaml_config(domain: str) -> dict[str, Any] | None:
    """Get the raw YAML configuration for a specific domain."""
    data = load_pipelines_yaml()
    return data.get("pipelines", {}).get(domain)
