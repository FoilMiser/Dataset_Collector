"""Integration tests for the pipeline specification system."""

from __future__ import annotations

import pytest

# Import registry to ensure all specs are registered
import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core.pipeline_factory import create_pipeline_driver, get_pipeline_driver
from collector_core.pipeline_spec import (
    PipelineSpec,
    get_pipeline_spec,
    list_pipelines,
)


class TestPipelineSpecRegistry:
    """Tests for pipeline specification registry."""

    def test_list_pipelines_returns_expected(self):
        domains = list_pipelines()
        assert len(domains) >= 15  # We registered 18 pipelines
        assert "chem" in domains
        assert "physics" in domains
        assert "math" in domains
        assert "nlp" in domains

    def test_get_pipeline_spec_returns_spec(self):
        spec = get_pipeline_spec("chem")
        assert spec is not None
        assert spec.domain == "chem"
        assert spec.name == "Chemistry Pipeline"
        assert spec.targets_yaml == "targets_chem.yaml"

    def test_get_pipeline_spec_returns_none_for_unknown(self):
        spec = get_pipeline_spec("nonexistent_domain")
        assert spec is None


class TestPipelineSpec:
    """Tests for PipelineSpec dataclass."""

    def test_prefix_defaults_to_domain(self):
        spec = PipelineSpec(
            domain="test",
            name="Test Pipeline",
            targets_yaml="targets_test.yaml",
        )
        assert spec.prefix == "test"

    def test_prefix_can_be_overridden(self):
        spec = PipelineSpec(
            domain="materials_science",
            name="Materials Science Pipeline",
            targets_yaml="targets_materials.yaml",
            domain_prefix="matsci",
        )
        assert spec.prefix == "matsci"

    def test_pipeline_id(self):
        spec = PipelineSpec(
            domain="test",
            name="Test Pipeline",
            targets_yaml="targets_test.yaml",
        )
        assert spec.pipeline_id == "test_pipeline_v2"

    def test_default_roots(self):
        spec = PipelineSpec(
            domain="test",
            name="Test Pipeline",
            targets_yaml="targets_test.yaml",
        )
        roots = spec.get_default_roots()
        assert roots["raw_root"] == "/data/test/raw"
        assert roots["logs_root"] == "/data/test/_logs"

    def test_default_roots_with_custom_prefix(self):
        spec = PipelineSpec(
            domain="bio",
            name="Biology Pipeline",
            targets_yaml="targets_bio.yaml",
            domain_prefix="biology",
        )
        roots = spec.get_default_roots()
        assert roots["raw_root"] == "/data/biology/raw"


class TestPipelineFactory:
    """Tests for pipeline factory."""

    def test_create_pipeline_driver_creates_class(self):
        spec = get_pipeline_spec("chem")
        driver_class = create_pipeline_driver(spec)

        assert driver_class is not None
        assert driver_class.DOMAIN == "chem"
        assert driver_class.TARGETS_LABEL == "targets_chem.yaml"

    def test_get_pipeline_driver_returns_class(self):
        driver_class = get_pipeline_driver("physics")
        assert driver_class is not None
        assert driver_class.DOMAIN == "physics"

    def test_get_pipeline_driver_raises_for_unknown(self):
        with pytest.raises(ValueError, match="Unknown pipeline domain"):
            get_pipeline_driver("nonexistent")


class TestAllPipelineSpecs:
    """Tests to validate all registered pipeline specs."""

    def test_all_specs_have_required_fields(self):
        for domain in list_pipelines():
            spec = get_pipeline_spec(domain)
            assert spec is not None, f"Failed to get spec for {domain}"
            assert spec.domain, f"Missing domain for {domain}"
            assert spec.name, f"Missing name for {domain}"
            assert spec.targets_yaml, f"Missing targets_yaml for {domain}"

    def test_all_specs_produce_valid_drivers(self):
        for domain in list_pipelines():
            driver_class = get_pipeline_driver(domain)
            assert driver_class is not None, f"Failed to create driver for {domain}"
            assert hasattr(driver_class, "DOMAIN")
            assert hasattr(driver_class, "TARGETS_LABEL")
