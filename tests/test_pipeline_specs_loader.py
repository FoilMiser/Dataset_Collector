"""Tests for pipeline_specs_loader."""

from __future__ import annotations

import pytest


class TestPipelineSpecsLoader:
    """Tests for loading pipeline specs from YAML."""

    def test_load_pipelines_yaml_returns_dict(self) -> None:
        """Test that load_pipelines_yaml returns a non-empty dict."""
        from collector_core.pipeline_specs_loader import load_pipelines_yaml

        data = load_pipelines_yaml()
        assert isinstance(data, dict)
        assert "pipelines" in data
        assert len(data["pipelines"]) > 0

    def test_load_pipeline_specs_from_yaml(self) -> None:
        """Test that pipeline specs are loaded correctly from YAML."""
        from collector_core.pipeline_specs_loader import load_pipeline_specs_from_yaml
        from collector_core.pipeline_spec import PipelineSpec

        specs = load_pipeline_specs_from_yaml()
        assert isinstance(specs, dict)
        assert len(specs) > 0

        # Check that all values are PipelineSpec objects
        for domain, spec in specs.items():
            assert isinstance(spec, PipelineSpec)
            assert spec.domain == domain
            assert spec.name  # Non-empty name
            assert spec.targets_yaml  # Non-empty targets
            assert spec.routing_keys  # Non-empty routing keys

    def test_chem_pipeline_spec_details(self) -> None:
        """Test specific details of the chem pipeline spec."""
        from collector_core.pipeline_specs_loader import load_pipeline_specs_from_yaml

        specs = load_pipeline_specs_from_yaml()
        chem = specs.get("chem")
        assert chem is not None
        assert chem.name == "Chemistry Pipeline"
        assert chem.targets_yaml == "targets_chem.yaml"
        assert "chem_routing" in chem.routing_keys
        assert chem.default_routing["subject"] == "chem"

    def test_pipeline_with_custom_workers(self) -> None:
        """Test that custom workers are loaded correctly."""
        from collector_core.pipeline_specs_loader import load_pipeline_specs_from_yaml

        specs = load_pipeline_specs_from_yaml()
        code = specs.get("code")
        assert code is not None
        assert "code_worker" in code.custom_workers
        assert code.custom_workers["code_worker"] == "code_worker.py"

    def test_pipeline_with_domain_prefix(self) -> None:
        """Test that domain_prefix is loaded correctly."""
        from collector_core.pipeline_specs_loader import load_pipeline_specs_from_yaml

        specs = load_pipeline_specs_from_yaml()
        matsci = specs.get("materials_science")
        assert matsci is not None
        assert matsci.domain_prefix == "matsci"
        assert matsci.prefix == "matsci"

    def test_pipeline_with_include_routing_dict(self) -> None:
        """Test that include_routing_dict_in_row is loaded correctly."""
        from collector_core.pipeline_specs_loader import load_pipeline_specs_from_yaml

        specs = load_pipeline_specs_from_yaml()
        metrology = specs.get("metrology")
        assert metrology is not None
        assert metrology.include_routing_dict_in_row is True


class TestRegistryIntegration:
    """Tests for the registry integration with the loader."""

    def test_registry_loaded_from_yaml(self) -> None:
        """Test that the registry is populated from YAML on import."""
        from collector_core.pipeline_specs_registry import list_pipelines, get_pipeline_spec

        pipelines = list_pipelines()
        assert len(pipelines) > 0
        assert "chem" in pipelines
        assert "physics" in pipelines

        # Verify spec is accessible
        chem = get_pipeline_spec("chem")
        assert chem is not None
        assert chem.name == "Chemistry Pipeline"

    def test_all_pipelines_have_valid_specs(self) -> None:
        """Test that all registered pipelines have valid specs."""
        from collector_core.pipeline_specs_registry import list_pipelines, get_pipeline_spec

        for domain in list_pipelines():
            spec = get_pipeline_spec(domain)
            assert spec is not None, f"Missing spec for domain: {domain}"
            assert spec.domain == domain
            assert spec.routing_keys, f"Empty routing_keys for: {domain}"
            assert spec.targets_yaml, f"Empty targets_yaml for: {domain}"


class TestValidatePipelineSpecs:
    """Tests for the validate_pipeline_specs tool."""

    def test_validate_all_pipelines_passes(self) -> None:
        """Test that validation passes for all pipelines."""
        from tools.validate_pipeline_specs import validate_all_pipelines

        error_count, errors = validate_all_pipelines()
        assert error_count == 0, f"Validation errors: {errors}"

    def test_validate_loader_consistency_passes(self) -> None:
        """Test that loader consistency validation passes."""
        from tools.validate_pipeline_specs import validate_loader_consistency

        error_count, errors = validate_loader_consistency()
        assert error_count == 0, f"Loader errors: {errors}"

    def test_validate_pipeline_config_catches_missing_fields(self) -> None:
        """Test that validation catches missing required fields."""
        from pathlib import Path
        from tools.validate_pipeline_specs import validate_pipeline_config

        # Config missing required fields
        bad_config: dict[str, object] = {}
        errors = validate_pipeline_config("test", bad_config, Path("."))

        assert len(errors) > 0
        assert any("pipeline_id" in e for e in errors)
        assert any("targets_path" in e for e in errors)
        assert any("routing" in e for e in errors)
