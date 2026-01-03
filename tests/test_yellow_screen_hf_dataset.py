import gzip
import json
import subprocess
import sys
from pathlib import Path

import yaml
from datasets import Dataset


def test_yellow_screen_hf_dataset(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    screened_root = tmp_path / "screened_yellow"
    manifests_root = tmp_path / "_manifests"
    ledger_root = tmp_path / "_ledger"
    pitches_root = tmp_path / "_pitches"

    target_id = "hf_target"
    pool = "permissive"
    raw_target_dir = raw_root / "yellow" / pool / target_id
    dataset_dir = raw_target_dir / "hf_dataset"

    dataset = Dataset.from_dict({"title": ["Hello"], "body": ["World"]})
    dataset.save_to_disk(dataset_dir)

    targets_cfg = {
        "globals": {
            "raw_root": str(raw_root),
            "screened_yellow_root": str(screened_root),
            "manifests_root": str(manifests_root),
            "ledger_root": str(ledger_root),
            "pitches_root": str(pitches_root),
            "screening": {
                "min_chars": 1,
                "max_chars": 1000,
                "text_field_candidates": ["text"],
            },
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

    worker_path = Path("kg_nav_pipeline_v2/yellow_screen_worker.py").resolve()
    subprocess.run(
        [
            sys.executable,
            str(worker_path),
            "--targets",
            str(targets_path),
            "--queue",
            str(queue_path),
            "--execute",
        ],
        check=True,
        cwd=Path(".").resolve(),
    )

    shard_path = screened_root / pool / "shards" / "yellow_shard_00000.jsonl.gz"
    assert shard_path.exists()

    with gzip.open(shard_path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle if line.strip()]

    assert len(rows) == 1
    record = rows[0]
    assert record["text"] == "Hello\nWorld"
    assert record["record_id"]
    assert record["source"]["license_profile"] == pool
    assert "hash" in record and record["hash"].get("content_sha256")

    ledger_path = ledger_root / "yellow_passed.jsonl"
    assert ledger_path.exists()
