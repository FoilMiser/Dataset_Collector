import gzip
import importlib.util
import json
import subprocess
from pathlib import Path

import yaml
from datasets import Dataset

OUTPUT_CONTRACT_PATH = Path(__file__).resolve().parents[1] / "tools" / "output_contract.py"
OUTPUT_CONTRACT_SPEC = importlib.util.spec_from_file_location("output_contract", OUTPUT_CONTRACT_PATH)
OUTPUT_CONTRACT_MODULE = importlib.util.module_from_spec(OUTPUT_CONTRACT_SPEC)
OUTPUT_CONTRACT_SPEC.loader.exec_module(OUTPUT_CONTRACT_MODULE)

REQUIRED_FIELDS = OUTPUT_CONTRACT_MODULE.REQUIRED_FIELDS
validate_output_contract = OUTPUT_CONTRACT_MODULE.validate_output_contract


def test_end_to_end_pipeline_contract(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    screened_root = tmp_path / "screened_yellow"
    combined_root = tmp_path / "combined"
    manifests_root = tmp_path / "_manifests"
    ledger_root = tmp_path / "_ledger"
    pitches_root = tmp_path / "_pitches"

    target_id = "contract_target"
    pool = "permissive"
    dataset_dir = raw_root / "yellow" / pool / target_id / "hf_dataset"

    dataset = Dataset.from_dict(
        {
            "title": ["Hello"],
            "body": ["World"],
            "source_url": ["https://example.test"],
        }
    )
    dataset.save_to_disk(dataset_dir)

    targets_cfg = {
        "globals": {
            "raw_root": str(raw_root),
            "screened_yellow_root": str(screened_root),
            "combined_root": str(combined_root),
            "manifests_root": str(manifests_root),
            "ledger_root": str(ledger_root),
            "pitches_root": str(pitches_root),
            "screening": {
                "min_chars": 1,
                "max_chars": 1000,
                "text_field_candidates": ["title", "body"],
            },
            "sharding": {"max_records_per_shard": 100, "compression": "gzip"},
        },
        "targets": [{"id": target_id}],
    }
    targets_path = tmp_path / "targets.yaml"
    targets_path.write_text(yaml.safe_dump(targets_cfg), encoding="utf-8")

    queue_path = tmp_path / "queue.jsonl"
    queue_path.write_text(
        json.dumps({"id": target_id, "license_profile": pool, "enabled": True}) + "\n",
        encoding="utf-8",
    )

    yellow_worker = Path("kg_nav_pipeline_v2/yellow_screen_worker.py").resolve()
    subprocess.run(
        [
            sys.executable,
            str(yellow_worker),
            "--targets",
            str(targets_path),
            "--queue",
            str(queue_path),
            "--execute",
        ],
        check=True,
        cwd=Path(".").resolve(),
    )

    merge_worker = Path("kg_nav_pipeline_v2/merge_worker.py").resolve()
    subprocess.run(
        [
            sys.executable,
            str(merge_worker),
            "--targets",
            str(targets_path),
            "--execute",
        ],
        check=True,
        cwd=Path(".").resolve(),
    )

    shard_paths = list(combined_root.glob("**/shards/*.jsonl.gz"))
    assert shard_paths

    with gzip.open(shard_paths[0], "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert rows
    record = rows[0]
    assert record["text"]
    assert record["record_id"]
    assert record["source"]["license_profile"] == pool
    assert record["hash"]["content_sha256"]
    validate_output_contract(record, "combined/shard")

    for field, expected_type in REQUIRED_FIELDS.items():
        assert field in record
        if field == "source_urls":
            assert isinstance(record[field], list)
            assert all(isinstance(item, str) for item in record[field])
        else:
            assert isinstance(record[field], expected_type)
