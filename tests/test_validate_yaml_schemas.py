from __future__ import annotations
from pathlib import Path

import pytest

from collector_core.config_validator import Draft7Validator
from collector_core.targets_paths import list_targets_files
from tools.validate_yaml_schemas import iter_companion_files, iter_pipeline_maps, validate_file


def test_validate_file_reports_yaml_parse_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("foo: [\n", encoding="utf-8")
    errors = validate_file(path, "targets")
    assert len(errors) == 1
    assert "YAML parse error" in errors[0]


@pytest.mark.skipif(Draft7Validator is None, reason="jsonschema not available")
def test_validate_file_reports_schema_error(tmp_path: Path) -> None:
    path = tmp_path / "targets.yaml"
    path.write_text("schema_version: '1'\ntargets:\n  - {}\n", encoding="utf-8")
    errors = validate_file(path, "targets")
    assert len(errors) == 1
    assert "Schema validation failed" in errors[0]


@pytest.mark.skipif(Draft7Validator is None, reason="jsonschema not available")
def test_validate_file_rejects_unknown_license_gates(tmp_path: Path) -> None:
    path = tmp_path / "targets.yaml"
    path.write_text(
        "schema_version: '0.9'\n"
        "targets:\n"
        "  - id: example\n"
        "    license_gates:\n"
        "      add: [unknown_gate]\n",
        encoding="utf-8",
    )
    errors = validate_file(path, "targets")
    assert len(errors) == 1
    assert "Schema validation failed" in errors[0]


@pytest.mark.skipif(Draft7Validator is None, reason="jsonschema not available")
def test_validate_file_rejects_unknown_content_checks(tmp_path: Path) -> None:
    path = tmp_path / "targets.yaml"
    path.write_text(
        "schema_version: '0.9'\n"
        "targets:\n"
        "  - id: example\n"
        "    content_checks:\n"
        "      add: [unknown_check]\n",
        encoding="utf-8",
    )
    errors = validate_file(path, "targets")
    assert len(errors) == 1
    assert "Schema validation failed" in errors[0]


@pytest.mark.skipif(Draft7Validator is None, reason="jsonschema not available")
def test_repo_yaml_schemas_validate() -> None:
    root = Path(__file__).resolve().parents[1]
    for targets_path in list_targets_files(root):
        assert validate_file(targets_path, "targets") == []
    for companion_path, schema in iter_companion_files(root):
        assert validate_file(companion_path, schema) == []
    for pipeline_map in iter_pipeline_maps(root):
        assert validate_file(pipeline_map, "pipeline_map") == []
