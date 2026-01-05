from __future__ import annotations

from pathlib import Path

import pytest

from collector_core.config_validator import Draft7Validator, read_yaml
from collector_core.exceptions import (
    ConfigValidationError,
    OutputPathsBuilderError,
    YamlParseError,
    DependencyMissingError,
)
from collector_core.pmc_worker import run_pmc_worker


def test_yaml_parse_error_includes_context(tmp_path: Path) -> None:
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("foo: [\n", encoding="utf-8")
    with pytest.raises(YamlParseError) as excinfo:
        read_yaml(bad_yaml)
    assert excinfo.value.code == "yaml_parse_error"
    assert excinfo.value.context["path"] == str(bad_yaml)


@pytest.mark.skipif(Draft7Validator is None, reason="jsonschema not available")
def test_config_validation_error_includes_context(tmp_path: Path) -> None:
    invalid = tmp_path / "targets.yaml"
    invalid.write_text("schema_version: '1'\n", encoding="utf-8")
    with pytest.raises(ConfigValidationError) as excinfo:
        read_yaml(invalid, schema_name="targets")
    assert excinfo.value.code == "config_validation_error"
    assert excinfo.value.context["schema"] == "targets"
    assert excinfo.value.context["path"] == str(invalid)
    assert excinfo.value.context["errors"]


def test_output_paths_builder_error(tmp_path: Path) -> None:
    targets_path = tmp_path / "targets.yaml"
    targets_path.write_text("schema_version: '1'\ntargets: []\n", encoding="utf-8")
    allowlist_path = tmp_path / "allowlist.jsonl"
    allowlist_path.write_text("", encoding="utf-8")
    with pytest.raises(OutputPathsBuilderError) as excinfo:
        run_pmc_worker(
            pipeline_id="pmc",
            pools_root_default="/tmp",
            log_dir_default="/tmp",
            include_pools_root_arg=False,
            args=[
                "--targets",
                str(targets_path),
                "--allowlist",
                str(allowlist_path),
            ],
        )
    assert excinfo.value.code == "output_paths_builder_required"


def test_dependency_missing_error_includes_install() -> None:
    err = DependencyMissingError(
        "missing dependency: demo",
        dependency="demo",
        install="pip install demo",
    )
    assert err.context["dependency"] == "demo"
    assert err.context["install"] == "pip install demo"
