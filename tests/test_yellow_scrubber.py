from __future__ import annotations

import gzip
import importlib.util
import json
import sys
from pathlib import Path


_SCRUBBER_SPEC = importlib.util.spec_from_file_location(
    "regcomp_yellow_scrubber",
    Path("regcomp_pipeline_v2/yellow_scrubber.py"),
)
assert _SCRUBBER_SPEC and _SCRUBBER_SPEC.loader
_SCRUBBER_MODULE = importlib.util.module_from_spec(_SCRUBBER_SPEC)
sys.modules[_SCRUBBER_SPEC.name] = _SCRUBBER_MODULE
_SCRUBBER_SPEC.loader.exec_module(_SCRUBBER_MODULE)

FieldSpec = _SCRUBBER_MODULE.FieldSpec
extract_pubchem_computed_only = _SCRUBBER_MODULE.extract_pubchem_computed_only
iter_sdf_records_from_gz = _SCRUBBER_MODULE.iter_sdf_records_from_gz
parse_sdf_tags = _SCRUBBER_MODULE.parse_sdf_tags
validate_record = _SCRUBBER_MODULE.validate_record


def _write_sdf_gz(path: Path, blocks: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for block in blocks:
            handle.write(block.strip("\n") + "\n")
            handle.write("$$$$\n")


def test_parse_sdf_tags_extracts_fields() -> None:
    record = "> <PUBCHEM_COMPOUND_CID>\n12345\n\n> <PUBCHEM_MOLECULAR_FORMULA>\nC6H6\n\n"
    tags = parse_sdf_tags(record)
    assert tags["PUBCHEM_COMPOUND_CID"] == "12345"
    assert tags["PUBCHEM_MOLECULAR_FORMULA"] == "C6H6"


def test_iter_sdf_records_from_gz_splits_records(tmp_path: Path) -> None:
    sdf_path = tmp_path / "sample.sdf.gz"
    blocks = [
        "> <PUBCHEM_COMPOUND_CID>\n1\n\n",
        "> <PUBCHEM_COMPOUND_CID>\n2\n\n",
    ]
    _write_sdf_gz(sdf_path, blocks)

    records = list(iter_sdf_records_from_gz(sdf_path))
    assert len(records) == 2
    assert "PUBCHEM_COMPOUND_CID" in records[0]
    assert "PUBCHEM_COMPOUND_CID" in records[1]


def test_extract_pubchem_computed_only_filters_fields(tmp_path: Path) -> None:
    quarantine_dir = tmp_path / "quarantine"
    permissive_dir = tmp_path / "permissive"
    quarantine_dir.mkdir()

    sdf_path = quarantine_dir / "pubchem.sdf.gz"
    blocks = [
        "> <PUBCHEM_COMPOUND_CID>\n42\n\n> <PUBCHEM_MOLECULAR_FORMULA>\nH2O\n\n",
    ]
    _write_sdf_gz(sdf_path, blocks)

    extract_pubchem_computed_only(
        quarantine_dir=quarantine_dir,
        permissive_out_dir=permissive_dir,
        include_globs=["*.sdf.gz"],
        include_fields=["PUBCHEM_COMPOUND_CID"],
        field_schema=None,
        shard_max_rows=10,
    )

    shard_path = permissive_dir / "shards" / "pubchem_computed_00000.jsonl.gz"
    assert shard_path.exists()

    with gzip.open(shard_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert rows == [{"PUBCHEM_COMPOUND_CID": "42"}]


def test_validate_record_reports_missing_required_field() -> None:
    schema = {
        "PUBCHEM_COMPOUND_CID": FieldSpec(
            name="PUBCHEM_COMPOUND_CID",
            field_type="integer",
            required=True,
        )
    }
    is_valid, errors = validate_record({}, schema)
    assert not is_valid
    assert "Missing required field: PUBCHEM_COMPOUND_CID" in errors


def test_validate_record_reports_non_nullable_empty() -> None:
    schema = {
        "PUBCHEM_MOLECULAR_FORMULA": FieldSpec(
            name="PUBCHEM_MOLECULAR_FORMULA",
            field_type="string",
            nullable=False,
        )
    }
    is_valid, errors = validate_record({"PUBCHEM_MOLECULAR_FORMULA": ""}, schema)
    assert not is_valid
    assert "Non-nullable field is empty: PUBCHEM_MOLECULAR_FORMULA" in errors


def test_math_routing_difficulty_level_regression() -> None:
    module_path = Path("math_pipeline_v2/pipeline_driver.py")
    spec = importlib.util.spec_from_file_location("math_pipeline_driver", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    MathPipelineDriver = module.MathPipelineDriver
    target = {"math_routing": {"level": 7}}
    extras = MathPipelineDriver().build_row_extras(target, routing={})
    assert extras["difficulty_level"] == 7
